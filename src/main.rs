use std::time::Duration;

use anyhow::Result;
use tokio::{sync::mpsc::{self, Sender, Receiver}, time::sleep};
use tracing::{info, Instrument};
use rand::{self, Rng, thread_rng};

pub const HASH_BYTE_SIZE: usize = 32;
pub type Sha256Bytes = [u8; HASH_BYTE_SIZE];

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub enum Message { 
    ClientMessage(ClientMessage), 
    EagerMessage(BodyMessage), 
    LazyMessage(HeaderMessage),
    PullMessage(HeaderMessage), 
    PruneMessage(IDMessage),
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub struct ClientMessage { 
    pub data: u32, 
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub struct BodyMessage { 
    pub data: u32, 
    pub sender_id: u32,
    pub msg_id: Sha256Bytes,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub struct HeaderMessage { 
    pub sender_id: u32,
    pub msg_id: Sha256Bytes,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub struct IDMessage { 
    pub sender_id: u32,
}

/* 
match message => {
    EagerMessage(msg, sender_id, msg_id) => {  // full msg
        if not recieved message_id { 
            add sender to eager_peers
        } else { 
            move sender to lazy_peers 
            send_prune(sender_id) 
        }

        eager_broadcast(msg)
        lazy_broadcast(msg_id) // using a schedule policy to reduce network load
    }, 
    PullMessage(sender_id, msg_id) => {  // req for full msg 
        move sender to eager_peers
        if have msg_id: 
            send(sender_id, msg, msg_id)
    }, 
    LazyMessage(sender_id, msg_id) => {  // just msg ID
        while true { 
            start timer for msg_id 
            when timer done check if msg_id has been eagerly recieved
            if not { 
                send_pull(sender_id, msg_id).await 
                sender_id = random(peers) // retry with another random peer
            } else { break }
        }
    }, 
    PruneMessage => { 
        move sender to lazy_peers 
    }
}
*/
pub async fn msg_reciever(id: usize, mut reciever: Receiver<Message>, world: Vec<Sender<Message>>) -> Result<()> { 
    // track what messages it has recieved so far 
    let mut msg_ids = vec![]; 
    let mut msg_data = vec![];

    // // track peers
    // let mut eager_peers = vec![];
    // let mut lazy_peers = vec![];

    while let Some(msg) = reciever.recv().await { 
        info!("recieved: {msg:?}");

        match msg { 
            Message::ClientMessage(msg) => { 
                // broadcast to eager 
                // broadcast to lazy 
            }
            Message::EagerMessage(msg) => {
                if !msg_ids.contains(&msg.msg_id) { 

                    // keep track 
                    msg_ids.push(msg.msg_id);
                    msg_data.push(msg.data);
                } else { 

                }

            }
            Message::LazyMessage(msg) => {}
            Message::PullMessage(msg) => {}
            Message::PruneMessage(msg) => {}
        }

    }
    Ok(())
}

pub async fn broadcast(peers: &Vec<Sender<Message>>, msg: Message) -> Result<()> { 
    info!("sending: {msg:?}");
    for peer in peers { 
        peer.send(msg).await?; // todo: move await out of loop
    }

    Ok(())
}

#[tokio::main(flavor="multi_thread", worker_threads=16)]
pub async fn main() { 
    tracing_subscriber::fmt::init();

    // !!
    let n_nodes = 2; 

    let world = (0..n_nodes).map(|_| mpsc::channel::<Message>(100)).collect::<Vec<_>>();
    let world_senders = world.iter().map(|(sender, _)| sender.clone()).collect::<Vec<_>>();

    let mut i = 0;
    for (_, reciever) in world {
        let mut peer_list = world_senders.clone();
        peer_list.remove(i);

        let _handle = tokio::spawn(async move { 
            msg_reciever(i, reciever, peer_list).await.unwrap();
        });

        i += 1;
    }

    // !! 
    let client_sleep_time: u64 = 3;

    let mut step = 0;
    loop { 
        let node_index: u64 = thread_rng().gen_range(0..n_nodes);
        let message = Message::ClientMessage(ClientMessage { data: step });
        let sender = &world_senders[node_index as usize];

        info!("Client sending {step:?}...");
        sender.send(message).await.unwrap();

        step += 1;
        sleep(Duration::from_secs(client_sleep_time)).await;
    }

}