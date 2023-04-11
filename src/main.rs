use std::{time::{Duration, SystemTime}, collections::hash_map::DefaultHasher};

use anyhow::Result;
use sha2::Sha256;
use sha2::Digest;
use tokio::{sync::mpsc::{self, Sender, Receiver}, time::sleep};
use tracing::info;
use rand::{self, Rng, thread_rng};

pub const HASH_BYTE_SIZE: usize = 4;
pub type Sha256Bytes = [u8; HASH_BYTE_SIZE];
pub type Node = (usize, Sender<Message>);

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
    pub sender_id: usize,
    pub msg_id: Sha256Bytes,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub struct HeaderMessage { 
    pub sender_id: usize,
    pub msg_id: Sha256Bytes,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub struct IDMessage { 
    pub sender_id: usize,
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

macro_rules! swap {
    ($id:expr, $src:ident $srcid:ident => $dst:ident $dstid:ident) => {
        || -> &Node {
            let index = $src.iter().position(|(id, _)| *id == $id).unwrap();
            let peer = $src.remove(index);
            let peer_id = $srcid.remove(index);
            $dst.push(peer);
            $dstid.push(peer_id);
            peer
        }
    };
}

pub async fn msg_reciever(id: usize, mut reciever: Receiver<Message>, peers: Vec<Node>) -> Result<()> { 
    let fanout_size = 2;

    let peer_ids = peers.iter().map(|(id, _)| *id).collect::<Vec<_>>();

    // track what messages it has recieved so far 
    let mut msg_ids = vec![]; 
    let mut msg_data = vec![];

    // track peers
    let n_nodes = peers.len();

    // first eager
    let mut eager_peers = vec![];
    let mut eager_ids = Vec::with_capacity(fanout_size);
    for _ in 0..fanout_size { 
        // sample without replacement 
        loop { 
            let node_index: usize = thread_rng().gen_range(0..n_nodes);
            let node_id = peer_ids[node_index];

            if !eager_ids.contains(&node_id) { 
                eager_ids.push(node_id);
                eager_peers.push(&peers[node_index]);
                break;
            }
        }
    }

    // then lazy the rest
    let mut lazy_peers = peers
        .iter()
        .filter(|(id, _)| !eager_ids.contains(id))
        .collect::<Vec<_>>();
    let mut lazy_ids = lazy_peers.iter().map(|(id, _)| *id).collect::<Vec<_>>();

    info!("NODE {id:?}: lazy {:?} eager {:?} total_n {:?}", lazy_ids, eager_ids, peers.len());
    assert!(lazy_peers.len() + eager_peers.len() == peers.len());
    assert!(lazy_peers.len() == lazy_ids.len());
    assert!(eager_peers.len() == eager_ids.len());

    // start
    while let Some(msg) = reciever.recv().await { 
        info!("NODE {id:?}: recieved: {msg:?}");

        match msg { 
            Message::ClientMessage(msg) => { 
                let time = std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                let mut hasher = Sha256::new();

                hasher.update(time.to_le_bytes());
                hasher.update(msg.data.to_le_bytes());
                let msg_id = hasher.finalize().as_slice()[..4].try_into().unwrap();

                msg_ids.push(msg_id);
                msg_data.push(msg.data);

                let eager_message = Message::EagerMessage(
                    BodyMessage { data: msg.data, sender_id: id, msg_id }
                );
                let lazy_message = Message::LazyMessage(
                    HeaderMessage { sender_id: id, msg_id }
                );

                info!("NODE {id:?}: broadcasting...");

                // broadcast to eager 
                broadcast(&eager_peers, eager_message).await?;
                // broadcast to lazy 
                broadcast(&lazy_peers, lazy_message).await?;

            }
            Message::EagerMessage(msg) => {
                let is_eager_sender = eager_ids.contains(&msg.sender_id);
                if !msg_ids.contains(&msg.msg_id) { 

                    // track message
                    msg_ids.push(msg.msg_id);
                    msg_data.push(msg.data);

                    info!("NODE {id:?}: broadcasting...");
                    let lazy_message = Message::LazyMessage(HeaderMessage { sender_id: id, msg_id: msg.msg_id});
                    let eager_message = Message::EagerMessage(BodyMessage { data: msg.data, sender_id: id, msg_id: msg.msg_id });

                    // exclude the sender which it was recieved from 
                    broadcast_exclude(&eager_peers, eager_message, msg.sender_id).await?;
                    broadcast_exclude(&lazy_peers, lazy_message, msg.sender_id).await?;

                    if !is_eager_sender { 
                        // move to eager
                        swap!(msg.sender_id, lazy_peers lazy_ids => eager_peers eager_ids)();
                        info!("NODE {id:?}: moving {:?} to eager...", msg.sender_id);
                        info!("NODE {id:?}: => lazy {:?} eager {:?}", lazy_ids, eager_ids);
                    } 

                } else if is_eager_sender { 
                    // move to lazy
                    let peer = swap!(msg.sender_id, eager_peers eager_ids => lazy_peers lazy_ids)();
                    info!("NODE {id:?}: moving {:?} to lazy...", msg.sender_id);
                    info!("NODE {id:?}: => lazy {:?} eager {:?}", lazy_ids, eager_ids);

                    // send prune
                    let message = Message::PruneMessage(IDMessage { sender_id: id });
                    peer.1.send(message).await?;
                }
            }
            Message::LazyMessage(msg) => {
                // TODO: need to do this and allow new information to flow in 
                let sleep_time = Duration::from_secs(1);
                sleep(sleep_time).await;

                // we have the info so the path is ok 
                if msg_ids.contains(&msg.msg_id) { continue; } 

                // need a new eager path bc we didnt get the data
                let message = Message::PullMessage(HeaderMessage { sender_id: id, msg_id: msg.msg_id });
                let (_, source_peer) = peers.iter().find(|(id, _)| *id == msg.sender_id).unwrap();
                source_peer.send(message).await?;
                
                if !eager_ids.contains(&msg.sender_id) { 
                    // add new link to eager
                    swap!(msg.sender_id, lazy_peers lazy_ids => eager_peers eager_ids)();
                    info!("NODE {id:?}: moving {:?} to eager...", msg.sender_id);
                    info!("NODE {id:?}: => lazy {:?} eager {:?}", lazy_ids, eager_ids);
                } 

                let sleep_time = Duration::from_secs(1);
                sleep(sleep_time).await;

                if msg_ids.contains(&msg.msg_id) { continue; } 
                panic!("NODE {id:?}: pull message failed abort!");
            }
            Message::PullMessage(msg) => {
                if !eager_ids.contains(&msg.sender_id) { 
                    // add new link to eager
                    swap!(msg.sender_id, lazy_peers lazy_ids => eager_peers eager_ids)();
                    info!("NODE {id:?}: moving {:?} to eager...", msg.sender_id);
                    info!("NODE {id:?}: => lazy {:?} eager {:?}", lazy_ids, eager_ids);
                } 

                if let Some(index) = msg_ids.iter().position(|msg_id| *msg_id == msg.msg_id) { 
                    info!("NODE {id:?}: replying to pull request...",);
                    let data = msg_data[index];
                    let message = Message::EagerMessage(BodyMessage { data: data, sender_id: id, msg_id: msg.msg_id });
                    let source_peer = eager_peers.iter().find(|(id, _)| *id == msg.sender_id).unwrap();
                    source_peer.1.send(message).await?;
                } else { 
                    panic!("NODE {id:?}: pull request message not found - abort!");
                }
            }
            Message::PruneMessage(msg) => {
                if eager_ids.contains(&msg.sender_id) { 
                    // move to lazy
                    swap!(msg.sender_id, eager_peers eager_ids => lazy_peers lazy_ids)();
                    info!("NODE {id:?}: moving {:?} to lazy...", msg.sender_id);
                    info!("NODE {id:?}: => lazy {:?} eager {:?}", lazy_ids, eager_ids);
                }
            }
        }

    }
    Ok(())
}

pub async fn broadcast_exclude(peers: &Vec<&Node>, msg: Message, exclude_id: usize) -> Result<()> { 
    for (id, peer) in peers { 
        if *id != exclude_id { 
            peer.send(msg).await?; // todo: move await out of loop
        }
    }

    Ok(())
}

pub async fn broadcast(peers: &Vec<&Node>, msg: Message) -> Result<()> { 
    for (_, peer) in peers { 
        peer.send(msg).await?; // todo: move await out of loop
    }
    Ok(())
}

#[tokio::main(flavor="multi_thread", worker_threads=16)]
pub async fn main() { 
    tracing_subscriber::fmt::init();

    // !!
    let n_nodes = 4; 

    let world = (0..n_nodes).map(|_| mpsc::channel::<Message>(100)).collect::<Vec<_>>();
    // (id, channel)
    let world_senders = world.iter().enumerate().map(|(i, (sender, _))| (i, sender.clone())).collect::<Vec<_>>();

    let mut i = 0;
    for (_, reciever) in world {
        let mut peers = world_senders.clone();
        peers.remove(i);

        let _handle = tokio::spawn(async move { 
            msg_reciever(i, reciever, peers).await.unwrap();
        });

        i += 1;
    }

    // !! 
    let client_sleep_time: u64 = 3;

    let mut step = 0;
    for _ in 0..1 { 
        let node_index: u64 = thread_rng().gen_range(0..n_nodes);
        let message = Message::ClientMessage(ClientMessage { data: step });
        let (_, sender) = &world_senders[node_index as usize];

        info!("Client sending {step:?}...");
        sender.send(message).await.unwrap();

        step += 1;
        sleep(Duration::from_secs(client_sleep_time)).await;
    }

}