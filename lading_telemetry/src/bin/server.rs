use lading_telemetry::protocol::{AsProto, ClientMessage, ContextAck, ValueKind};
use prost::Message;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = UnixListener::bind("/tmp/telemetry.sock")?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let (mut reader, mut writer) = socket.split();

            let mut reader = BufReader::new(&mut reader);
            let mut writer = BufWriter::new(&mut writer);

            let mut read_buf = Vec::new();
            let mut write_buf = Vec::new();
            let mut context_map: HashMap<u32, String> = HashMap::new();
            let mut context_id_counter = 1;

            loop {
                // Read the size prefix of the incoming message from the wire,
                // always a u32.
                let prefix = reader.read_u32().await.unwrap() as usize;
                read_buf.clear();
                read_buf.resize(prefix, 0);

                // Read the encoded ClientMessage from the wire into buffer,
                // decode. Exit the loop and the task if the read is of zero
                // size.
                assert!(read_buf.capacity() >= prefix);
                let n = reader.read_exact(&mut read_buf[..prefix]).await.unwrap();
                assert!(n == prefix);
                if n == 0 {
                    break;
                }
                let message = ClientMessage::decode(&read_buf[0..prefix]).unwrap();

                match message {
                    ClientMessage::ContextRegistration(context) => {
                        let context_name = context.metric_name();
                        println!("Received registration for {}", context_name);
                        let id = match context_map.entry(context_id_counter) {
                            Entry::Occupied(ent) => *ent.key(),
                            Entry::Vacant(ent) => {
                                let _ = ent.insert(String::from(context_name));
                                let id = context_id_counter;
                                context_id_counter += 1;
                                id
                            }
                        };

                        let context_ack = ContextAck::builder().context_id(id).build();

                        // Serialize the ContextAck and then write it on the
                        // wire. Note we have to serialize and calculate the
                        // prefix size as we do not rely on prost's delimiting.
                        let proto = context_ack.to_proto();
                        let delim = proto.encoded_len();
                        write_buf.clear();
                        write_buf.reserve(delim);
                        proto.encode(&mut write_buf).unwrap();

                        writer.write_u32(delim as u32).await.unwrap();
                        writer.write_all(&write_buf[..delim]).await.unwrap();
                        writer.flush().await.unwrap();
                    }
                    ClientMessage::SignalMessage(signal) => {
                        let context_id = signal.context_id();
                        let epoch_second = signal.epoch_second();
                        let kind = signal.value_kind();
                        let value = signal.value();

                        match kind {
                            ValueKind::Gauge => {
                                println!(
                                    "Received gauge value: {} for context ID: {} at epoch second: {}",
                                    value, context_id, epoch_second
                                );
                            }
                            ValueKind::Counter => {
                                println!(
                                    "Received counter value: {} for context ID: {} at epoch second: {}",
                                    value, context_id, epoch_second
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}
