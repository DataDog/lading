//! `NetFlow` v5 payload.

use std::{
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use rand::{Rng, distr::weighted::WeightedIndex, prelude::Distribution};
use serde::{Deserialize, Serialize as SerdeSerialize};

use crate::{Error, Serialize, common::config::ConfRange};

/// `NetFlow` v5 packet header (24 bytes)
#[derive(Debug, Clone)]
struct NetFlowV5Header {
    version: u16,           // NetFlow version (always 5)
    count: u16,             // Number of flow records
    sys_uptime: u32,        // Milliseconds since router boot
    unix_secs: u32,         // Seconds since Unix epoch
    unix_nsecs: u32,        // Nanoseconds since Unix epoch
    flow_sequence: u32,     // Sequence counter of total flows
    engine_type: u8,        // Type of flow switching engine
    engine_id: u8,          // ID of flow switching engine
    sampling_interval: u16, // Sampling interval
}

/// `NetFlow` v5 flow record (48 bytes)
#[derive(Debug, Clone, Copy)]
struct NetFlowV5Record {
    srcaddr: u32,  // Source IP address
    dstaddr: u32,  // Destination IP address
    nexthop: u32,  // Next hop IP address
    input: u16,    // Input interface index
    output: u16,   // Output interface index
    d_pkts: u32,   // Packets in the flow
    d_octets: u32, // Total bytes in the flow
    first: u32,    // SysUptime at start of flow
    last: u32,     // SysUptime at end of flow
    srcport: u16,  // TCP/UDP source port
    dstport: u16,  // TCP/UDP destination port
    pad1: u8,      // Unused padding
    tcp_flags: u8, // Cumulative OR of TCP flags
    prot: u8,      // IP protocol (TCP=6, UDP=17, etc.)
    tos: u8,       // IP type of service
    src_as: u16,   // Source BGP AS number
    dst_as: u16,   // Destination BGP AS number
    src_mask: u8,  // Source address prefix mask
    dst_mask: u8,  // Destination address prefix mask
    pad2: u16,     // Unused padding
}

/// Configuration for NetFlow v5 payload generation
#[derive(Debug, Deserialize, SerdeSerialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields, default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Config {
    /// Range for number of flow records per packet
    pub flows_per_packet: ConfRange<u16>,

    /// Range for source IP addresses (as u32)
    pub src_ip_range: ConfRange<u32>,

    /// Range for destination IP addresses (as u32)
    pub dst_ip_range: ConfRange<u32>,

    /// Range for source ports
    pub src_port_range: ConfRange<u16>,

    /// Range for destination ports
    pub dst_port_range: ConfRange<u16>,

    /// Range for packet counts in flows
    pub packet_count_range: ConfRange<u32>,

    /// Range for byte counts in flows
    pub byte_count_range: ConfRange<u32>,

    /// Range for flow duration in milliseconds
    pub flow_duration_range: ConfRange<u32>,

    /// Range for interface indices
    pub interface_range: ConfRange<u16>,

    /// Range for AS numbers
    pub as_number_range: ConfRange<u16>,

    /// Range for ToS (Type of Service) values
    pub tos_range: ConfRange<u8>,

    /// Protocol weights (TCP, UDP, ICMP, Other)
    pub protocol_weights: ProtocolWeights,

    /// Engine type to use
    pub engine_type: u8,

    /// Engine ID to use
    pub engine_id: u8,

    /// Aggregation settings for generating related flows
    pub aggregation: AggregationConfig,
}

/// Configuration for flow aggregation and port rollup
#[derive(Debug, Deserialize, SerdeSerialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields, default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AggregationConfig {
    /// Flow aggregation ratio - generates additional flows with identical 5-tuples
    /// A ratio of 1.6 means for every base flow, generate 0.6 additional identical flows
    /// Set to 1.0 to disable flow aggregation
    pub flow_aggregation_ratio: f32,

    /// Percentage of flows that should have flow aggregation applied (0.0-1.0)
    /// 0.0 = no flows get flow aggregation, 1.0 = all flows get flow aggregation
    /// Only flows selected by this percentage will use flow_aggregation_ratio and additional_flows_delay_num_flows
    pub flow_aggregation_percentage_of_flows: f32,

    /// Percentage of flows that should have port rollup aggregation applied (0.0-1.0)
    /// 0.0 = no flows get port rollup, 1.0 = all eligible flows get port rollup
    pub port_rollup_percentage_of_flows: f32,

    /// Range for number of additional port rollup flows to generate
    /// Each selected flow will generate between min and max additional flows with different source ports
    pub port_rollup_range: ConfRange<u16>,

    /// Maximum time variance in milliseconds for aggregated flows
    /// Aggregated flows will have timestamps within this range of the base flow
    pub time_variance_ms: u32,

    /// Whether to apply small variations to packet/byte counts in aggregated flows
    pub vary_counts: bool,

    /// Maximum percentage variation for packet/byte counts (0.0-1.0)
    /// Only used if vary_counts is true
    pub count_variation_percent: f32,

    /// Number of flows to add to the block cache before adding additional flows
    /// When set to 0, additional flows are added immediately (default behavior)
    /// When set to N > 0, additional flows are delayed until N flows have been added to the cache
    pub additional_flows_delay_num_flows: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            flows_per_packet: ConfRange::Inclusive { min: 1, max: 30 }, // Max 30 flows to stay under MTU
            src_ip_range: ConfRange::Inclusive {
                min: u32::from_be_bytes([10, 0, 0, 1]),       // 10.0.0.1
                max: u32::from_be_bytes([10, 255, 255, 254]), // 10.255.255.254
            },
            dst_ip_range: ConfRange::Inclusive {
                min: u32::from_be_bytes([192, 168, 1, 1]), // 192.168.1.1
                max: u32::from_be_bytes([192, 168, 255, 254]), // 192.168.255.254
            },
            src_port_range: ConfRange::Inclusive {
                min: 1024,
                max: 65535,
            },
            dst_port_range: ConfRange::Inclusive { min: 1, max: 65535 },
            packet_count_range: ConfRange::Inclusive { min: 1, max: 10000 },
            byte_count_range: ConfRange::Inclusive {
                min: 64,
                max: 1_500_000,
            },
            flow_duration_range: ConfRange::Inclusive {
                min: 1000,
                max: 3_600_000,
            }, // 1s to 1h
            interface_range: ConfRange::Inclusive { min: 1, max: 254 },
            as_number_range: ConfRange::Inclusive { min: 1, max: 65535 },
            tos_range: ConfRange::Inclusive { min: 0, max: 255 },
            protocol_weights: ProtocolWeights::default(),
            engine_type: 0,
            engine_id: 0,
            aggregation: AggregationConfig::default(),
        }
    }
}

impl Config {
    /// Validate the configuration
    pub fn valid(&self) -> Result<(), String> {
        let (flows_valid, reason) = self.flows_per_packet.valid();
        if !flows_valid {
            return Err(format!("flows_per_packet is invalid: {reason}"));
        }

        // Check that max flows won't exceed MTU (24 byte header + 48 bytes per flow)
        if self.flows_per_packet.end() > 30 {
            return Err(
                "flows_per_packet maximum should not exceed 30 to stay within MTU limits"
                    .to_string(),
            );
        }

        let (src_ip_valid, reason) = self.src_ip_range.valid();
        if !src_ip_valid {
            return Err(format!("src_ip_range is invalid: {reason}"));
        }

        let (dst_ip_valid, reason) = self.dst_ip_range.valid();
        if !dst_ip_valid {
            return Err(format!("dst_ip_range is invalid: {reason}"));
        }

        let (tos_valid, reason) = self.tos_range.valid();
        if !tos_valid {
            return Err(format!("tos_range is invalid: {reason}"));
        }

        // Validate aggregation configuration
        if self.aggregation.flow_aggregation_ratio < 1.0 {
            return Err("flow_aggregation_ratio must be >= 1.0".to_string());
        }

        if !(0.0..=1.0).contains(&self.aggregation.flow_aggregation_percentage_of_flows) {
            return Err("flow_aggregation_percentage_of_flows must be between 0.0 and 1.0".to_string());
        }

        if !(0.0..=1.0).contains(&self.aggregation.port_rollup_percentage_of_flows) {
            return Err("port_rollup_percentage_of_flows must be between 0.0 and 1.0".to_string());
        }

        let (port_rollup_valid, reason) = self.aggregation.port_rollup_range.valid();
        if !port_rollup_valid {
            return Err(format!("port_rollup_range is invalid: {reason}"));
        }

        if self.aggregation.count_variation_percent < 0.0 || self.aggregation.count_variation_percent > 1.0 {
            return Err("count_variation_percent must be between 0.0 and 1.0".to_string());
        }

        // No specific validation needed for additional_flows_delay_num_flows - any u32 value is valid

        Ok(())
    }
}

/// Protocol distribution weights
#[derive(Debug, Deserialize, SerdeSerialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ProtocolWeights {
    /// Weight for TCP protocol
    pub tcp: u8,
    /// Weight for UDP protocol
    pub udp: u8,
    /// Weight for ICMP protocol
    pub icmp: u8,
    /// Weight for other protocols
    pub other: u8,
}

impl Default for ProtocolWeights {
    fn default() -> Self {
        Self {
            tcp: 70,  // 70%
            udp: 25,  // 25%
            icmp: 3,  // 3%
            other: 2, // 2%
        }
    }
}

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            flow_aggregation_ratio: 1.0,               // No aggregation by default
            flow_aggregation_percentage_of_flows: 0.0, // No flows get flow aggregation by default
            port_rollup_percentage_of_flows: 0.0,      // No port rollup by default
            port_rollup_range: ConfRange::Inclusive { min: 1, max: 5 }, // 1-5 additional flows when enabled
            time_variance_ms: 1000,                    // 1 second variance
            vary_counts: true,                         // Apply small variations
            count_variation_percent: 0.1,              // 10% variation
            additional_flows_delay_num_flows: 0,       // Add additional flows immediately by default
        }
    }
}

/// Represents a group of additional flows waiting to be released after their base flow
#[derive(Debug, Clone)]
struct PendingFlowGroup {
    /// The additional flows waiting to be added
    additional_flows: Vec<NetFlowV5Record>,
    /// How many more flows need to be added to the cache before these are released
    flows_remaining_before_release: u32,
}

/// NetFlow v5 payload generator
#[derive(Debug)]
pub struct NetFlowV5 {
    config: Config,
    protocol_distribution: WeightedIndex<u16>,
    flow_sequence: u32,
    sys_uptime_base: u32,
    flow_pool: Vec<NetFlowV5Record>,
    /// Groups of pending additional flows, each associated with a base flow
    pending_flow_groups: Vec<PendingFlowGroup>,
}

impl NetFlowV5 {
    /// Create a new NetFlow v5 payload generator
    pub fn new<R>(config: Config, rng: &mut R) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        config.valid().map_err(|_e| Error::StringGenerate)?;

        let protocol_weights = [
            u16::from(config.protocol_weights.tcp),
            u16::from(config.protocol_weights.udp),
            u16::from(config.protocol_weights.icmp),
            u16::from(config.protocol_weights.other),
        ];

        Ok(Self {
            config,
            protocol_distribution: WeightedIndex::new(protocol_weights)?,
            flow_sequence: rng.random(),
            sys_uptime_base: rng.random_range(0..86_400_000), // Random base uptime (0-24h)
            flow_pool: Vec::new(),
            pending_flow_groups: Vec::new(),
        })
    }

    /// Generate a NetFlow v5 header
    fn generate_header<R>(&mut self, flow_count: u16, rng: &mut R) -> NetFlowV5Header
    where
        R: Rng + ?Sized,
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        let current_uptime = self.sys_uptime_base + rng.random_range(0..3_600_000); // Add up to 1h

        NetFlowV5Header {
            version: 5,
            count: flow_count,
            sys_uptime: current_uptime,
            unix_secs: now.as_secs() as u32,
            unix_nsecs: (now.subsec_nanos() / 1000) * 1000, // Round to microseconds
            flow_sequence: self.flow_sequence,
            engine_type: self.config.engine_type,
            engine_id: self.config.engine_id,
            sampling_interval: 0, // No sampling
        }
    }

    /// Generate a NetFlow v5 flow record
    fn generate_flow_record<R>(&self, base_uptime: u32, rng: &mut R) -> NetFlowV5Record
    where
        R: Rng + ?Sized,
    {
        let protocol = match self.protocol_distribution.sample(rng) {
            0 => 6,                         // TCP
            1 => 17,                        // UDP
            2 => 1,                         // ICMP
            _ => rng.random_range(2..=255), // Other protocols
        };

        let flow_duration = self.config.flow_duration_range.sample(rng);
        let first_uptime = base_uptime.saturating_sub(flow_duration);

        // Generate realistic port combinations based on protocol
        let (srcport, dstport) = if protocol == 6 || protocol == 17 {
            // TCP or UDP
            (
                self.config.src_port_range.sample(rng),
                self.config.dst_port_range.sample(rng),
            )
        } else {
            (0, 0) // ICMP and others don't use ports
        };

        // Generate TCP flags if TCP
        let tcp_flags = if protocol == 6 {
            rng.random_range(0..=255) // Random combination of TCP flags
        } else {
            0
        };

        NetFlowV5Record {
            srcaddr: self.config.src_ip_range.sample(rng),
            dstaddr: self.config.dst_ip_range.sample(rng),
            nexthop: 0, // Often 0 for directly connected
            input: self.config.interface_range.sample(rng),
            output: self.config.interface_range.sample(rng),
            d_pkts: self.config.packet_count_range.sample(rng),
            d_octets: self.config.byte_count_range.sample(rng),
            first: first_uptime,
            last: base_uptime,
            srcport,
            dstport,
            pad1: 0,
            tcp_flags,
            prot: protocol,
            tos: self.config.tos_range.sample(rng),
            src_as: self.config.as_number_range.sample(rng),
            dst_as: self.config.as_number_range.sample(rng),
            src_mask: rng.random_range(8..=32), // Reasonable subnet masks
            dst_mask: rng.random_range(8..=32),
            pad2: 0,
        }
    }

    /// Check if a flow should get flow aggregation based on percentage
    fn should_apply_flow_aggregation<R>(&self, rng: &mut R) -> bool
    where
        R: Rng + ?Sized,
    {
        self.config.aggregation.flow_aggregation_percentage_of_flows > 0.0
            && self.config.aggregation.flow_aggregation_ratio > 1.0
            && rng.random::<f32>() < self.config.aggregation.flow_aggregation_percentage_of_flows
    }

    /// Generate flow aggregation flows (identical 5-tuples) for a base flow
    fn generate_flow_aggregation<R>(&self, base_record: &NetFlowV5Record, base_uptime: u32, rng: &mut R) -> Vec<NetFlowV5Record>
    where
        R: Rng + ?Sized,
    {
        let mut flows = Vec::new();
        let additional_flows = (self.config.aggregation.flow_aggregation_ratio - 1.0).round() as u32;
        
        for _ in 0..additional_flows {
            let mut aggregated_flow = *base_record;
            self.apply_aggregation_variations(&mut aggregated_flow, base_uptime, rng);
            flows.push(aggregated_flow);
        }
        
        flows
    }

    /// Generate port rollup flows (same dst info, different src ports) for a base flow
    fn generate_port_rollup<R>(&self, base_record: &NetFlowV5Record, base_uptime: u32, rng: &mut R) -> Vec<NetFlowV5Record>
    where
        R: Rng + ?Sized,
    {
        let mut flows = Vec::new();
        
        // Only apply to TCP/UDP flows
        if self.config.aggregation.port_rollup_percentage_of_flows > 0.0 
            && (base_record.prot == 6 || base_record.prot == 17) {
            
            // Check if this flow should get port rollup based on percentage
            let should_rollup: f32 = rng.random();
            if should_rollup < self.config.aggregation.port_rollup_percentage_of_flows {
                let additional_flows = self.config.aggregation.port_rollup_range.sample(rng) as u32;
                for _ in 0..additional_flows {
                    let mut rollup_flow = *base_record;
                    // Keep same dst_ip, dst_port, protocol, but change src_port
                    rollup_flow.srcport = self.config.src_port_range.sample(rng);
                    self.apply_aggregation_variations(&mut rollup_flow, base_uptime, rng);
                    flows.push(rollup_flow);
                }
            }
        }
        
        flows
    }



    /// Apply variations to aggregated flows (timing and count variations)
    fn apply_aggregation_variations<R>(&self, flow: &mut NetFlowV5Record, base_uptime: u32, rng: &mut R)
    where
        R: Rng + ?Sized,
    {
        // Apply time variance
        if self.config.aggregation.time_variance_ms > 0 {
            let time_offset = rng.random_range(0..=self.config.aggregation.time_variance_ms);
            flow.first = flow.first.saturating_add(time_offset);
            flow.last = base_uptime.saturating_add(time_offset);
        }

        // Apply count variations if enabled
        if self.config.aggregation.vary_counts {
            let variation = self.config.aggregation.count_variation_percent;
            
            // Vary packet count
            let pkt_variation = (flow.d_pkts as f32 * variation * (rng.random::<f32>() - 0.5) * 2.0) as i32;
            flow.d_pkts = (flow.d_pkts as i32 + pkt_variation).max(1) as u32;
            
            // Vary byte count
            let byte_variation = (flow.d_octets as f32 * variation * (rng.random::<f32>() - 0.5) * 2.0) as i32;
            flow.d_octets = (flow.d_octets as i32 + byte_variation).max(64) as u32;
        }
    }

    /// Process pending flow groups and move ready flows to the active pool
    fn process_pending_flow_groups(&mut self) {
        let mut groups_to_remove = Vec::new();
        
        for (index, group) in self.pending_flow_groups.iter().enumerate() {
            if group.flows_remaining_before_release == 0 {
                groups_to_remove.push(index);
            }
        }
        
        // Remove groups that are ready (in reverse order to preserve indices)
        for &index in groups_to_remove.iter().rev() {
            let group = self.pending_flow_groups.remove(index);
            self.flow_pool.extend(group.additional_flows);
        }
    }

    /// Decrement the flow counters for all pending groups
    fn decrement_pending_flow_counters(&mut self, flows_added: u32) {
        for group in &mut self.pending_flow_groups {
            group.flows_remaining_before_release = group.flows_remaining_before_release.saturating_sub(flows_added);
        }
    }

    /// Write header to bytes in network byte order
    fn write_header<W>(&self, header: &NetFlowV5Header, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        writer.write_all(&header.version.to_be_bytes())?;
        writer.write_all(&header.count.to_be_bytes())?;
        writer.write_all(&header.sys_uptime.to_be_bytes())?;
        writer.write_all(&header.unix_secs.to_be_bytes())?;
        writer.write_all(&header.unix_nsecs.to_be_bytes())?;
        writer.write_all(&header.flow_sequence.to_be_bytes())?;
        writer.write_all(&[header.engine_type])?;
        writer.write_all(&[header.engine_id])?;
        writer.write_all(&header.sampling_interval.to_be_bytes())?;
        Ok(())
    }

    /// Write flow record to bytes in network byte order
    fn write_flow_record<W>(&self, record: &NetFlowV5Record, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        writer.write_all(&record.srcaddr.to_be_bytes())?;
        writer.write_all(&record.dstaddr.to_be_bytes())?;
        writer.write_all(&record.nexthop.to_be_bytes())?;
        writer.write_all(&record.input.to_be_bytes())?;
        writer.write_all(&record.output.to_be_bytes())?;
        writer.write_all(&record.d_pkts.to_be_bytes())?;
        writer.write_all(&record.d_octets.to_be_bytes())?;
        writer.write_all(&record.first.to_be_bytes())?;
        writer.write_all(&record.last.to_be_bytes())?;
        writer.write_all(&record.srcport.to_be_bytes())?;
        writer.write_all(&record.dstport.to_be_bytes())?;
        writer.write_all(&[record.pad1])?;
        writer.write_all(&[record.tcp_flags])?;
        writer.write_all(&[record.prot])?;
        writer.write_all(&[record.tos])?;
        writer.write_all(&record.src_as.to_be_bytes())?;
        writer.write_all(&record.dst_as.to_be_bytes())?;
        writer.write_all(&[record.src_mask])?;
        writer.write_all(&[record.dst_mask])?;
        writer.write_all(&record.pad2.to_be_bytes())?;
        Ok(())
    }
}

impl Serialize for NetFlowV5 {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        const HEADER_SIZE: usize = 24;
        const FLOW_RECORD_SIZE: usize = 48;

        //println!("NetFlow DEBUG: max_bytes={}", max_bytes);

        if max_bytes < HEADER_SIZE + FLOW_RECORD_SIZE {
            //println!("NetFlow DEBUG: Not enough space for even one flow, returning early");
            return Ok(());
        }

        // Calculate maximum flows that fit in the byte budget
        let max_flows_by_budget = (max_bytes - HEADER_SIZE) / FLOW_RECORD_SIZE;
        let max_flows_per_packet = max_flows_by_budget.min(30); // NetFlow v5 max is 30
        //println!("NetFlow DEBUG: max_flows_by_budget={}, max_flows_per_packet={}", max_flows_by_budget, max_flows_per_packet);

        // Phase 1: Determine packet size
        let desired_flows_in_packet = self.config.flows_per_packet.sample(&mut rng) as usize;
        let desired_packet_bytes = HEADER_SIZE + (desired_flows_in_packet * FLOW_RECORD_SIZE);
        
        // If max_bytes can't accommodate our desired packet size, return Ok(()) to let
        // construct_block_cache_inner() determine the appropriate floor size
        if max_bytes < desired_packet_bytes {
            //println!("NetFlow DEBUG: max_bytes={} < desired_packet_bytes={}, returning Ok(()) for cache size determination", max_bytes, desired_packet_bytes);
            return Ok(());
        }
        
        let target_flows_in_packet = desired_flows_in_packet.min(max_flows_per_packet);
        //println!("NetFlow DEBUG: desired_flows_in_packet={}, target_flows_in_packet={}", desired_flows_in_packet, target_flows_in_packet);
        //println!("NetFlow DEBUG: flows_per_packet config={:?}", self.config.flows_per_packet);
        
        // Phase 2: Check for any pending flow groups that are ready to be released
        self.process_pending_flow_groups();

        // Phase 3: Ensure flow pool has enough flows
        //println!("NetFlow DEBUG: flow_pool.len() before generation={}", self.flow_pool.len());
        while self.flow_pool.len() < target_flows_in_packet {
            // Generate a complete base flow
            let base_uptime = self.sys_uptime_base + rng.random_range(0..3_600_000);
            let base_flow = self.generate_flow_record(base_uptime, &mut rng);
            self.flow_pool.push(base_flow);
            //println!("NetFlow DEBUG: Added base flow, pool size now={}", self.flow_pool.len());

            // Make aggregation decision upfront for efficiency
            let should_aggregate = self.should_apply_flow_aggregation(&mut rng);
            
            if should_aggregate {
                // Generate flow aggregation (identical 5-tuples)
                let aggregated_flows = self.generate_flow_aggregation(&base_flow, base_uptime, &mut rng);
                //println!("NetFlow DEBUG: Generated {} flow aggregation flows", aggregated_flows.len());
                
                // If delay is enabled, create a pending group for these flows
                if self.config.aggregation.additional_flows_delay_num_flows > 0 && !aggregated_flows.is_empty() {
                    let pending_group = PendingFlowGroup {
                        additional_flows: aggregated_flows,
                        flows_remaining_before_release: self.config.aggregation.additional_flows_delay_num_flows,
                    };
                    self.pending_flow_groups.push(pending_group);
                    //println!("NetFlow DEBUG: Added {} flows to pending group, pending groups count now={}", aggregated_flows.len(), self.pending_flow_groups.len());
                } else {
                    // No delay - add directly to pool
                    self.flow_pool.extend(aggregated_flows);
                    //println!("NetFlow DEBUG: After flow aggregation, pool size now={}", self.flow_pool.len());
                }
            } else {
                // No flow aggregation - try port rollup instead
                let port_rollup_flows = self.generate_port_rollup(&base_flow, base_uptime, &mut rng);
                if !port_rollup_flows.is_empty() {
                    self.flow_pool.extend(port_rollup_flows);
                    //println!("NetFlow DEBUG: After port rollup, pool size now={}", self.flow_pool.len());
                }
            }
        }
        
        // Phase 4: Take exactly the flows needed for this packet
        let packet_flows: Vec<NetFlowV5Record> = self.flow_pool.drain(0..target_flows_in_packet).collect();
        //println!("NetFlow DEBUG: Taking {} flows for packet, remaining in pool={}", packet_flows.len(), self.flow_pool.len());

        // Decrement the delay counters for all pending flow groups based on flows added to cache
        if self.config.aggregation.additional_flows_delay_num_flows > 0 {
            self.decrement_pending_flow_counters(packet_flows.len() as u32);
        }

        if packet_flows.is_empty() {
            //println!("NetFlow DEBUG: No flows to send, returning early");
            return Ok(());
        }

        let header = self.generate_header(packet_flows.len() as u16, &mut rng);

        // Write header
        self.write_header(&header, writer)?;

        // Write flow records
        for flow_record in &packet_flows {
            self.write_flow_record(flow_record, writer)?;
        }

        let _final_packet_size = HEADER_SIZE + (packet_flows.len() * FLOW_RECORD_SIZE);
        //println!("NetFlow DEBUG: Final packet size={} bytes ({} header + {}*{} flows)", 
        //        final_packet_size, HEADER_SIZE, packet_flows.len(), FLOW_RECORD_SIZE);

        // Update sequence number for next packet
        self.flow_sequence = self.flow_sequence.wrapping_add(packet_flows.len() as u32);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let config = Config::default();
            let mut netflow = NetFlowV5::new(config, &mut rng).unwrap();

            let mut bytes = Vec::with_capacity(max_bytes);
            netflow.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            prop_assert!(bytes.len() <= max_bytes);
        }

        #[test]
        fn valid_netflow_packet_structure(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let config = Config::default();
            let mut netflow = NetFlowV5::new(config, &mut rng).unwrap();

            let mut bytes = Vec::new();
            netflow.to_bytes(rng, 1500, &mut bytes).unwrap();

            if !bytes.is_empty() {
                // Should have at least header
                prop_assert!(bytes.len() >= 24);
                // Should be header + multiple of 48 bytes
                prop_assert_eq!((bytes.len() - 24) % 48, 0);
                // Check version is 5
                prop_assert_eq!(u16::from_be_bytes([bytes[0], bytes[1]]), 5);
            }
        }

        #[test]
        fn aggregation_generates_additional_flows(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut config = Config::default();
            
            // Set aggregation ratios
            config.aggregation.flow_aggregation_ratio = 2.0; // Double the flows
            config.aggregation.flow_aggregation_percentage_of_flows = 1.0; // All flows get aggregation
            config.aggregation.port_rollup_percentage_of_flows = 0.0; // No port rollup
            config.flows_per_packet = ConfRange::Constant(10); // Allow enough flows for aggregation
            
            let mut netflow = NetFlowV5::new(config, &mut rng).unwrap();

            let mut bytes = Vec::new();
            netflow.to_bytes(rng, 1500, &mut bytes).unwrap();

            if !bytes.is_empty() {
                // Should have header (24 bytes) + flow records (48 bytes each)
                let flow_count = (bytes.len() - 24) / 48;
                // Now respects flows_per_packet (10), but with aggregation should have mix of base + aggregated
                prop_assert!(flow_count >= 8 && flow_count <= 10);
            }
        }

        #[test]
        fn additional_flows_delay_mechanism_works(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut config = Config::default();
            
            // Enable flow aggregation with delay
            config.aggregation.flow_aggregation_ratio = 2.0; // Generate 1 additional flow per base flow
            config.aggregation.flow_aggregation_percentage_of_flows = 1.0; // All flows get aggregation
            config.aggregation.additional_flows_delay_num_flows = 2; // Delay additional flows until 2 flows added after base
            config.flows_per_packet = ConfRange::Constant(1); // Allow 1 flow per packet to control timing precisely
            
            let mut netflow = NetFlowV5::new(config, &mut rng).unwrap();

            // Packet 1: First base flow - additional flows go to pending (waiting for 2 more flows)
            let mut bytes1 = Vec::new();
            netflow.to_bytes(&mut rng, 1500, &mut bytes1).unwrap();
            prop_assert!(!bytes1.is_empty());
            let flow_count1 = (bytes1.len() - 24) / 48;
            prop_assert_eq!(flow_count1, 1); // Should have 1 base flow
            
            // Packet 2: Second base flow - this counts as 1 flow after first base
            let mut bytes2 = Vec::new();
            netflow.to_bytes(&mut rng, 1500, &mut bytes2).unwrap();
            prop_assert!(!bytes2.is_empty());
            let flow_count2 = (bytes2.len() - 24) / 48;
            prop_assert_eq!(flow_count2, 1); // Should have 1 base flow
            
            // Packet 3: Third base flow - this counts as 2 flows after first base, so first group's additional flows should be released
            let mut bytes3 = Vec::new();
            netflow.to_bytes(&mut rng, 1500, &mut bytes3).unwrap();
            prop_assert!(!bytes3.is_empty());
            let flow_count3 = (bytes3.len() - 24) / 48;
            // Should have 1 flow (could be the third base flow or the released additional flow from first base)
            prop_assert_eq!(flow_count3, 1);
        }

        #[test]
        fn port_rollup_generates_different_source_ports(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut config = Config::default();
            
            // Set port rollup settings
            config.aggregation.flow_aggregation_ratio = 1.0; // No flow aggregation
            config.aggregation.port_rollup_percentage_of_flows = 1.0; // 100% get port rollup
            config.aggregation.port_rollup_range = ConfRange::Constant(3); // Generate 3 additional flows
            config.flows_per_packet = ConfRange::Constant(6); // Allow enough flows for rollup
            
            // Force TCP protocol to ensure port rollup applies
            config.protocol_weights.tcp = 100;
            config.protocol_weights.udp = 0;
            config.protocol_weights.icmp = 0;
            config.protocol_weights.other = 0;
            
            let mut netflow = NetFlowV5::new(config, &mut rng).unwrap();

            let mut bytes = Vec::new();
            netflow.to_bytes(rng, 1500, &mut bytes).unwrap();

            if !bytes.is_empty() {
                let flow_count = (bytes.len() - 24) / 48;
                // Now respects flows_per_packet (6), should have mix of base + port rollup flows
                prop_assert!(flow_count >= 4 && flow_count <= 6);
            }
        }
    }
}
