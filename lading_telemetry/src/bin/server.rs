use lading_telemetry::protocol::{
    AsProto, ClientMessage, ContextAck, ContextRegistration, ValueKind,
};
use prost::Message;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

#[derive(thiserror::Error, Debug)]
enum Error {}

#[derive(Debug, Default)]
struct Context {
    next_id: u32,
    id_to_hash: HashMap<u32, u64>,
    map: HashMap<u64, (ContextRegistration, u32)>,
}

impl Context {
    /// Given the context ID, return the `ContextRegistration``.
    fn get(&self, id: u32) -> Option<&ContextRegistration> {
        self.id_to_hash
            .get(&id)
            .and_then(|hash| self.map.get(hash).map(|(ctx, _)| ctx))
    }

    /// Store the `ContextRegistration` and return the context ID.
    ///
    /// If the context is already registered, return the existing context ID.
    fn store(&mut self, ctx: ContextRegistration) -> u32 {
        // Compute the hash for the ctx allowing us to determine if there's an
        // assigned ID or not.
        let mut hasher = DefaultHasher::new();
        ctx.hash(&mut hasher);
        let hash: u64 = hasher.finish();

        // Check `map` to see if the context is already registered, return the
        // ID if so. If not, assign a new ID and update the hash to ID map to
        // allow `get`.
        match self.map.entry(hash) {
            Entry::Occupied(ent) => ent.get().1,
            Entry::Vacant(ent) => {
                let id = self.next_id;
                self.next_id += 1;
                let _ = ent.insert((ctx, id));
                self.id_to_hash.insert(id, hash);
                id
            }
        }
    }
}

struct Server {
    path: PathBuf,
    context: Arc<Mutex<Context>>,
}

impl Server {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            context: Arc::new(Mutex::new(Context::default())),
        }
    }

    async fn spin(self) {
        let listener = UnixListener::bind(&self.path).unwrap();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let ctx = self.context.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, ctx).await {
                    eprintln!("Error: {:?}", e);
                }
            });
        }
    }
}

async fn handle_connection(mut socket: UnixStream, ctx: Arc<Mutex<Context>>) -> Result<(), Error> {
    let (mut reader, mut writer) = socket.split();

    let mut reader = BufReader::new(&mut reader);
    let mut writer = BufWriter::new(&mut writer);

    let mut read_buf = Vec::new();
    let mut write_buf = Vec::new();

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
            break Ok(());
        }
        let message = ClientMessage::decode(&read_buf[0..prefix]).unwrap();

        match message {
            ClientMessage::ContextRegistration(context) => {
                let context_name = context.metric_name();
                println!("Received registration for {}", context_name);
                let id = ctx.lock().await.store(context);

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
                        let guard = ctx.lock().await;
                        let metric_name = guard.get(context_id).unwrap().metric_name();
                        println!(
                            "[{context_id}](gauge) {metric_name} -- {value} @ {epoch_second}."
                        );
                    }
                    ValueKind::Counter => {
                        let guard = ctx.lock().await;
                        let metric_name = guard.get(context_id).unwrap().metric_name();
                        println!(
                            "[{context_id}](counter) {metric_name} -- {value} @ {epoch_second}."
                        )
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let path = PathBuf::from("/tmp/telemetry.sock");
    let server = Server::new(path);

    server.spin().await;

    Ok(())
}
