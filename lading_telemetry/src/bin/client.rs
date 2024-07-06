use std::time::{SystemTime, UNIX_EPOCH};

use lading_telemetry::protocol::{AsProto, ContextRegistration, ServerMessage};
use prost::Message;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;

fn get_current_epoch_seconds() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_secs()
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut stream = UnixStream::connect("/tmp/telemetry.sock").await?;
    let (mut reader, mut writer) = stream.split();

    let mut reader = BufReader::new(&mut reader);
    let mut writer = BufWriter::new(&mut writer);

    let mut read_buf: Vec<u8> = vec![0; 1024];
    let mut write_buf: Vec<u8> = vec![0; 1024];

    // Send our context registration messages and then wait for the acks
    {
        let reg1 = ContextRegistration::builder()
            .metric_name("cpu_usage")
            .labels(&[("host", "server1")])
            .build();

        let reg2 = ContextRegistration::builder()
            .metric_name("memory_usage")
            .labels(&[("host", "server1")])
            .build();

        {
            let proto = reg1.to_proto();
            let delim = proto.encoded_len();
            write_buf.clear();
            proto.encode(&mut write_buf).unwrap();
            writer.write_u32(delim as u32).await.unwrap();
            writer.write_all(&write_buf[..delim]).await.unwrap();
        }
        {
            let proto = reg2.to_proto();
            let delim = proto.encoded_len();
            write_buf.clear();
            proto.encode(&mut write_buf).unwrap();
            writer.write_u32(delim as u32).await.unwrap();
            writer.write_all(&write_buf[..delim]).await.unwrap();
        }
    }
    // Flush to avoid stalling forever
    writer.flush().await.unwrap();

    let cpu_usage_context_id;
    let memory_usage_context_id;

    // NOTE these very raw calls to the socket etc suggest a higher level abstraction
    {
        // Read the prefix size of the incoming message from the wire, always a
        // u32.
        let prefix = reader.read_u32().await.unwrap() as usize;
        read_buf.clear();
        read_buf.resize(prefix, 0);

        // In a real implementation we would need to handle acks interleaved
        // with sending out signal messages but we're only one client so it's no
        // big deal for now.
        let n = reader.read_exact(&mut read_buf[..prefix]).await.unwrap();
        assert!(n == prefix);

        let message = ServerMessage::decode(&read_buf[..prefix]).unwrap();
        match message {
            ServerMessage::ContextAck(ack) => {
                cpu_usage_context_id = ack.context_id();
            }
        }
    }

    // NOTE we are assuming in-order call / response
    {
        // Read the prefix size of the incoming message from the wire, always a
        // u32.
        let prefix = reader.read_u32().await.unwrap() as usize;
        read_buf.clear();
        read_buf.resize(prefix, 0);

        // In a real implementation we would need to handle acks interleaved
        // with sending out signal messages but we're only one client so it's no
        // big deal for now.
        let n = reader.read_exact(&mut read_buf[..prefix]).await.unwrap();
        assert!(n == prefix);

        let message = ServerMessage::decode(&read_buf[..prefix]).unwrap();
        match message {
            ServerMessage::ContextAck(ack) => {
                memory_usage_context_id = ack.context_id();
            }
        }
    }

    // Now, send signal messages for our two registered contexts
    loop {
        {
            // The CPU usage signal message
            let signal_message = lading_telemetry::protocol::SignalMessage::builder()
                .context_id(cpu_usage_context_id)
                .epoch_second(get_current_epoch_seconds())
                .value_kind(lading_telemetry::protocol::ValueKind::Gauge)
                .value(37.7)
                .build();

            let proto = signal_message.to_proto();
            let delim = proto.encoded_len();
            write_buf.clear();
            proto.encode(&mut write_buf).unwrap();
            writer.write_u32(delim as u32).await.unwrap();
            writer
                .write_all(&write_buf[..delim as usize])
                .await
                .unwrap();
        }

        {
            // The memory usage signal message
            let signal_message = lading_telemetry::protocol::SignalMessage::builder()
                .context_id(memory_usage_context_id)
                .epoch_second(get_current_epoch_seconds())
                .value_kind(lading_telemetry::protocol::ValueKind::Gauge)
                .value(37.7)
                .build();

            let proto = signal_message.to_proto();
            let delim = proto.encoded_len();
            write_buf.clear();
            proto.encode(&mut write_buf).unwrap();
            writer.write_u32(delim as u32).await.unwrap();
            writer
                .write_all(&write_buf[..delim as usize])
                .await
                .unwrap();
        }
    }
}
