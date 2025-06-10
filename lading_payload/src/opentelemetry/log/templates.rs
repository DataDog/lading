use std::rc::Rc;

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    resource::v1::Resource,
};
use prost::Message;
use rand::{
    Rng,
    distr::{Distribution, weighted::WeightedIndex},
};

use super::Config;
use crate::opentelemetry::common::templates::Pool as GenericPool;
use crate::opentelemetry::common::{GeneratorError, TagGenerator, UNIQUE_TAG_RATIO};
use crate::{Error, common::config::ConfRange, common::strings};

pub(crate) type Pool = GenericPool<ResourceLogs, ResourceTemplateGenerator>;

#[derive(Debug, Clone)]
pub(crate) struct LogTemplateGenerator {
    severity_dist: WeightedIndex<u16>,
    str_pool: Rc<strings::Pool>,
    tags: TagGenerator,
    body_size: ConfRange<u16>,
    trace_ids: Vec<Vec<u8>>,
}

impl LogTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::Pool>,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let tags = TagGenerator::new(
            rng.random(),
            config.contexts.attributes_per_log,
            ConfRange::Inclusive { min: 3, max: 32 },
            config.contexts.total_contexts.end() as usize,
            Rc::clone(str_pool),
            UNIQUE_TAG_RATIO,
        )?;

        let trace_count = config.trace_cardinality.sample(rng) as usize;
        let mut trace_ids = Vec::with_capacity(trace_count);
        for _ in 0..trace_count {
            let mut trace_id = vec![0u8; 16];
            rng.fill_bytes(&mut trace_id);
            trace_ids.push(trace_id);
        }

        Ok(Self {
            // NOTE if you change the ordering here update the code below that
            // sets `severity_number` to match. If indexes DO NOT match we will
            // not generate the right severity.
            severity_dist: WeightedIndex::new([
                u16::from(config.severity_weights.trace),
                u16::from(config.severity_weights.debug),
                u16::from(config.severity_weights.warn),
                u16::from(config.severity_weights.error),
                u16::from(config.severity_weights.fatal),
                u16::from(config.severity_weights.info),
            ])?,
            str_pool: Rc::clone(str_pool),
            tags,
            body_size: config.body_size,
            trace_ids,
        })
    }
}

impl<'a> crate::SizedGenerator<'a> for LogTemplateGenerator {
    type Output = LogRecord;
    type Error = GeneratorError;

    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: Rng + ?Sized,
    {
        let original_budget: usize = *budget;
        let mut inner_budget: usize = *budget;

        let attributes = match self.tags.generate(rng, &mut inner_budget) {
            Ok(attrs) => attrs,
            Err(GeneratorError::SizeExhausted) => {
                if inner_budget == original_budget {
                    return Err(GeneratorError::SizeExhausted);
                }
                Vec::new()
            }
            Err(e) => return Err(e),
        };

        let severity_idx = self.severity_dist.sample(rng);
        let severity_number = match severity_idx {
            0 => SeverityNumber::Trace as i32,
            1 => SeverityNumber::Debug as i32,
            2 => SeverityNumber::Warn as i32,
            3 => SeverityNumber::Error as i32,
            4 => SeverityNumber::Fatal as i32,
            _ => SeverityNumber::Info as i32,
        };

        let body_size = self.body_size.sample(rng);
        let body = self
            .str_pool
            .of_size(rng, body_size as usize)
            .ok_or(GeneratorError::StringGenerate)?;

        let trace_id = if self.trace_ids.is_empty() {
            Vec::new()
        } else {
            let idx = rng.random_range(0..self.trace_ids.len());
            self.trace_ids[idx].clone()
        };

        let log_record = LogRecord {
            time_unix_nano: 0,
            observed_time_unix_nano: 0,
            severity_number,
            severity_text: String::new(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(body.to_string())),
            }),
            attributes,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id,
            span_id: Vec::new(),
            event_name: String::new(),
        };

        let encoded_size = log_record.encoded_len();
        if encoded_size > inner_budget {
            return Err(GeneratorError::SizeExhausted);
        }

        *budget = original_budget.saturating_sub(encoded_size);
        Ok(log_record)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ScopeTemplateGenerator {
    log_gen: LogTemplateGenerator,
    tags: TagGenerator,
    str_pool: Rc<strings::Pool>,
    logs_per_scope: ConfRange<u8>,
}

impl ScopeTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::Pool>,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let tags = TagGenerator::new(
            rng.random(),
            config.contexts.attributes_per_scope,
            ConfRange::Inclusive { min: 3, max: 32 },
            config.contexts.total_contexts.end() as usize,
            Rc::clone(str_pool),
            UNIQUE_TAG_RATIO,
        )?;

        Ok(Self {
            log_gen: LogTemplateGenerator::new(config, str_pool, rng)?,
            tags,
            str_pool: Rc::clone(str_pool),
            logs_per_scope: config.contexts.logs_per_scope,
        })
    }
}

impl<'a> crate::SizedGenerator<'a> for ScopeTemplateGenerator {
    type Output = ScopeLogs;
    type Error = GeneratorError;

    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: Rng + ?Sized,
    {
        let original_budget: usize = *budget;
        let mut inner_budget: usize = *budget;

        let scope_attributes = match self.tags.generate(rng, &mut inner_budget) {
            Ok(attrs) => attrs,
            Err(GeneratorError::SizeExhausted) => {
                if inner_budget == original_budget {
                    return Err(GeneratorError::SizeExhausted);
                }
                Vec::new()
            }
            Err(e) => return Err(e),
        };

        let scope = if scope_attributes.is_empty() {
            None
        } else {
            Some(InstrumentationScope {
                name: self
                    .str_pool
                    .of_size_range(rng, 1_u16..32_u16)
                    .ok_or(GeneratorError::StringGenerate)?
                    .to_string(),
                version: String::new(),
                attributes: scope_attributes,
                dropped_attributes_count: 0,
            })
        };

        let num_logs = self.logs_per_scope.sample(rng);
        let mut log_records = Vec::with_capacity(num_logs as usize);

        for _ in 0..num_logs {
            match self.log_gen.generate(rng, &mut inner_budget) {
                Ok(log) => log_records.push(log),
                Err(GeneratorError::SizeExhausted) => {
                    if log_records.is_empty() {
                        return Err(GeneratorError::SizeExhausted);
                    }
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        let scope_logs = ScopeLogs {
            scope,
            log_records,
            schema_url: String::new(),
        };

        let encoded_size = scope_logs.encoded_len();
        if encoded_size > inner_budget {
            return Err(GeneratorError::SizeExhausted);
        }

        *budget -= encoded_size;
        Ok(scope_logs)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResourceTemplateGenerator {
    scope_gen: ScopeTemplateGenerator,
    tags: TagGenerator,
    scopes_per_resource: ConfRange<u8>,
}

impl ResourceTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::Pool>,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        config.valid().map_err(Error::Validation)?;

        let tags = TagGenerator::new(
            rng.random(),
            config.contexts.attributes_per_resource,
            ConfRange::Inclusive { min: 3, max: 32 },
            config.contexts.total_contexts.end() as usize,
            Rc::clone(str_pool),
            UNIQUE_TAG_RATIO,
        )?;

        Ok(Self {
            scope_gen: ScopeTemplateGenerator::new(config, str_pool, rng)?,
            tags,
            scopes_per_resource: config.contexts.scopes_per_resource,
        })
    }
}

impl<'a> crate::SizedGenerator<'a> for ResourceTemplateGenerator {
    type Output = ResourceLogs;
    type Error = GeneratorError;

    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: Rng + ?Sized,
    {
        let original_budget: usize = *budget;
        let mut inner_budget: usize = *budget;

        let resource_attributes = match self.tags.generate(rng, &mut inner_budget) {
            Ok(attrs) => attrs,
            Err(GeneratorError::SizeExhausted) => {
                if inner_budget == original_budget {
                    return Err(GeneratorError::SizeExhausted);
                }
                Vec::new()
            }
            Err(e) => return Err(e),
        };

        let resource = if resource_attributes.is_empty() {
            None
        } else {
            Some(Resource {
                attributes: resource_attributes,
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            })
        };

        let num_scopes = self.scopes_per_resource.sample(rng);
        let mut scope_logs = Vec::with_capacity(num_scopes as usize);

        for _ in 0..num_scopes {
            match self.scope_gen.generate(rng, &mut inner_budget) {
                Ok(scope) => scope_logs.push(scope),
                Err(GeneratorError::SizeExhausted) => {
                    if scope_logs.is_empty() {
                        return Err(GeneratorError::SizeExhausted);
                    }
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        let resource_logs = ResourceLogs {
            resource,
            scope_logs,
            schema_url: String::new(),
        };

        let encoded_size = resource_logs.encoded_len();
        if encoded_size > original_budget {
            return Err(GeneratorError::SizeExhausted);
        }

        *budget -= encoded_size;
        Ok(resource_logs)
    }
}
