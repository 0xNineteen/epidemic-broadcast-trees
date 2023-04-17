use std::{time::{Duration, SystemTime}, sync::Arc};

use anyhow::Result;
use sha2::Sha256;
use sha2::Digest;
use tokio::{sync::mpsc::{self, Sender, Receiver}, time::{sleep, interval}};
use tracing::info;
use rand::{self, Rng, thread_rng};

pub const HASH_BYTE_SIZE: usize = 4;
pub type Sha256Bytes = [u8; HASH_BYTE_SIZE];
pub type Peer = (usize, Sender<Message>);

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
    pub header: HeaderMessage,
    pub data: u32, 
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

macro_rules! swap {
    ($id:expr, $src:expr, $srcid:expr => $dst:expr, $dstid:expr) => {
        || -> Arc<Peer> {
            let index = $src.iter().position(|peer| peer.0 == $id).unwrap();
            let peer = $src.remove(index);
            let peer_id = $srcid.remove(index);
            $dst.push(peer.clone());
            $dstid.push(peer_id);
            peer
        }
    };
}

struct Node { 
    reciever: Receiver<Message>,
    id: usize,
    msg_data: Vec<u32>,
    msg_ids: Vec<[u8; 4]>,
    //
    eager_peers: Vec<Arc<Peer>>, 
    eager_ids: Vec<usize>, 
    //
    lazy_peers: Vec<Arc<Peer>>, 
    lazy_ids: Vec<usize>, 
}

impl Node { 
    pub fn new(id: usize, reciever: Receiver<Message>, peers: Vec<Arc<Peer>>) -> Self { 
        // ! 
        let fanout_size = 2;
        let peer_ids = peers.iter().map(|peer| peer.0).collect::<Vec<_>>();
    
        // track peers
        let n_nodes = peers.len();
    
        // first eager (fanout size)
        let mut eager_peers = vec![];
        let mut eager_ids = Vec::with_capacity(fanout_size);
        for _ in 0..fanout_size { 
            // sample without replacement 
            loop { 
                let node_index: usize = thread_rng().gen_range(0..n_nodes);
                let node_id = peer_ids[node_index];
    
                if !eager_ids.contains(&node_id) { 
                    eager_ids.push(node_id);
                    eager_peers.push(peers[node_index].clone());
                    break;
                }
            }
        }
    
        // lazy the rest
        let lazy_peers = peers
            .iter()
            .filter(|peer| !eager_ids.contains(&peer.0))
            .map(|peer| peer.clone())
            .collect::<Vec<_>>();
        let lazy_ids = lazy_peers.iter().map(|peer| peer.0).collect::<Vec<_>>();
    
        info!("NODE {id:?}: lazy {:?} eager {:?} total_n {:?}", lazy_ids, eager_ids, peers.len());
        assert!(lazy_peers.len() + eager_peers.len() == peers.len());
        assert!(lazy_peers.len() == lazy_ids.len());
        assert!(eager_peers.len() == eager_ids.len());

        // track what messages it has recieved so far 
        let msg_ids = vec![]; 
        let msg_data = vec![];

        Node { 
            reciever, 
            id, 
            msg_data, 
            msg_ids, 
            eager_peers, 
            eager_ids, 
            lazy_peers, 
            lazy_ids, 
        }
    }

    pub fn move_to_lazy(&mut self, id: usize) -> Arc<Peer> { 
        let peer = swap!(id, self.eager_peers, self.eager_ids => self.lazy_peers, self.lazy_ids)();
        info!("NODE {:?}: moving {:?} to lazy...", self.id, id);
        info!("NODE {:?}: => lazy {:?} eager {:?}", self.id, self.lazy_ids, self.eager_ids);
        peer
    }

    pub fn move_to_eager(&mut self, id: usize) -> Arc<Peer> { 
        let peer = swap!(id, self.lazy_peers, self.lazy_ids => self.eager_peers, self.eager_ids)();
        info!("NODE {:?}: moving {:?} to eager...", self.id, id);
        info!("NODE {:?}: => lazy {:?} eager {:?}", self.id, self.lazy_ids, self.eager_ids);
        peer
    }

    pub async fn handle_eager_message(&mut self, msg: BodyMessage) -> Result<()> { 
        let BodyMessage { 
            header: HeaderMessage { sender_id, msg_id }, 
            data
        } = msg;

        let is_eager_sender = self.eager_ids.contains(&sender_id);
        if !self.msg_ids.contains(&msg_id) { 

            // track message
            self.msg_ids.push(msg_id);
            self.msg_data.push(data);

            info!("NODE {:?}: broadcasting...", self.id);
            let header_msg = HeaderMessage { sender_id: self.id, msg_id}; 
            let lazy_message = Message::LazyMessage(header_msg);
            let eager_message = Message::EagerMessage(BodyMessage { data, header: header_msg});

            // exclude the sender which it was recieved from 
            broadcast_exclude(&self.eager_peers, eager_message, sender_id).await?;
            broadcast_exclude(&self.lazy_peers, lazy_message, sender_id).await?;

            if !is_eager_sender { 
                // move to eager
                self.move_to_eager(sender_id);
            } 

        } else if is_eager_sender { 
            let peer = self.move_to_lazy(sender_id);
            // send prune
            let message = Message::PruneMessage(IDMessage { sender_id: self.id });
            peer.1.send(message).await?;
        }

        Ok(())
    }

    pub async fn handle_pull_message(&mut self, msg: HeaderMessage) -> Result<()> { 
        if !self.eager_ids.contains(&msg.sender_id) { 
            // add new link to eager
            self.move_to_eager(msg.sender_id);
        } 
        let HeaderMessage { msg_id: req_msg_id, .. } = msg;

        if let Some(index) = self.msg_ids.iter().position(|msg_id| *msg_id == req_msg_id) { 
            info!("NODE {:?}: replying to pull request...", self.id);
            let data = self.msg_data[index];

            // create the response
            let header_msg = HeaderMessage { sender_id: self.id, msg_id: req_msg_id }; 
            let message = Message::EagerMessage(BodyMessage { data, header: header_msg });
            let source_peer = self.eager_peers.iter().find(|peer| peer.0 == msg.sender_id).unwrap();
            source_peer.1.send(message).await?;
        } else { 
            panic!("NODE {:?}: pull request message not found - abort!", self.id);
        }

        Ok(())
    }

    pub async fn handle_prune_message(&mut self, msg: IDMessage) -> Result<()> { 
        if self.eager_ids.contains(&msg.sender_id) { 
            self.move_to_lazy(msg.sender_id);
        }

        Ok(())
    }

    pub async fn handle_client_message(&mut self, msg: ClientMessage) -> Result<()> { 
        let time = std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
        let mut hasher = Sha256::new();

        hasher.update(time.to_le_bytes());
        hasher.update(msg.data.to_le_bytes());
        let msg_id = hasher.finalize().as_slice()[..4].try_into().unwrap();

        self.msg_ids.push(msg_id);
        self.msg_data.push(msg.data);

        let header_message = HeaderMessage { sender_id: self.id, msg_id }; 
        let eager_message = Message::EagerMessage(
            BodyMessage { data: msg.data, header: header_message }
        );
        let lazy_message = Message::LazyMessage(
            header_message
        );

        info!("NODE {:?}: broadcasting...", self.id);

        // broadcast to eager 
        broadcast(&self.eager_peers, eager_message).await?;
        // broadcast to lazy 
        broadcast(&self.lazy_peers, lazy_message).await?;

        Ok(())
    }

    pub async fn recieve(&mut self) -> anyhow::Result<()> { 
        let mut missing_messages: Vec<HeaderMessage> = vec![];
        let mut check_tick = interval(Duration::from_secs(1000)); // placeholder
        let mut state_tick = interval(Duration::from_secs(3)); 

        // start
        loop { 
            tokio::select! {
                _ = state_tick.tick() => { 
                    info!("NODE {:?}: state = {:?}", self.id, self.msg_data.last());
                }
                _ = check_tick.tick() => { 
                    if let Some(mut msg) = missing_messages.pop() { 
                        if !self.msg_ids.contains(&msg.msg_id) { 
                            // todo: remove the peer which never eager sent
                            // need a new eager path bc we didnt get the data
                            let message = Message::PullMessage(HeaderMessage { sender_id: self.id, msg_id: msg.msg_id });
                            let source_peer = {
                                let peer = self.lazy_peers.iter().find(|peer| peer.0 == msg.sender_id);
                                if peer.is_none() { 
                                    self.eager_peers.iter().find(|peer| peer.0 == msg.sender_id).unwrap()
                                } else { 
                                    peer.unwrap()
                                }
                            };
                            source_peer.1.send(message).await?;
                            
                            if !self.eager_ids.contains(&msg.sender_id) { 
                                // add new link to eager
                                self.move_to_eager(msg.sender_id);
                            } 
                            
                            // change the sender id to a new random node (in case this one fails too)
                            let node_index: usize = thread_rng().gen_range(0..self.lazy_ids.len());
                            msg.sender_id = self.lazy_ids[node_index];
                            missing_messages.push(msg);

                        } else { 
                            info!("NODE {:?}: pull request succeeded!", self.id);
                        }
                    }

                    if missing_messages.len() == 0 { 
                        // reset tick
                        check_tick = interval(Duration::from_secs(1000)); // placeholder
                    }
                }
                resp = self.reciever.recv() => { 
                    if resp.is_none() { return Ok(()) }
                    let msg = resp.unwrap();
                    info!("NODE {:?}: recieved: {msg:?}", self.id);

                    match msg { 
                        Message::ClientMessage(msg) => { self.handle_client_message(msg).await? }
                        Message::EagerMessage(msg) => { self.handle_eager_message(msg).await? }
                        Message::LazyMessage(msg) => {
                            // wait for missing message
                            missing_messages.push(msg);
                            let sleep_time = Duration::from_secs(2);
                            check_tick = interval(sleep_time);
                            let _ = check_tick.tick().await; // scratch first tick
                        }
                        Message::PullMessage(msg) => { self.handle_pull_message(msg).await? }
                        Message::PruneMessage(msg) => { self.handle_prune_message(msg).await? }
                    }
                }
            }
        }
    }
}

pub async fn broadcast_exclude(peers: &Vec<Arc<Peer>>, msg: Message, exclude_id: usize) -> Result<()> { 
    for peer in peers { 
        if peer.0 != exclude_id { 
            peer.1.send(msg).await?; // todo: move await out of loop
        }
    }

    Ok(())
}

pub async fn broadcast(peers: &Vec<Arc<Peer>>, msg: Message) -> Result<()> { 
    for peer in peers { 
        peer.1.send(msg).await?; // todo: move await out of loop
    }

    Ok(())
}

// pub async fn setup_world(n_nodes: usize) -> (Vec<Node>, Vec<Peer>) {

//     (world, world_senders)
// }

#[tokio::main(flavor="multi_thread", worker_threads=16)]
pub async fn main() { 
    tracing_subscriber::fmt::init();

    // !!
    let n_nodes = 4; 

    let world = (0..n_nodes).map(|_| mpsc::channel::<Message>(100)).collect::<Vec<_>>();
    let world_senders = world.iter().enumerate().map(|(id, (sender, _))| Arc::new((id, sender.clone()))).collect::<Vec<_>>();

    // setup every node
    let mut i = 0;
    for (_, reciever) in world {
        let mut peers = world_senders.clone();
        peers.remove(i);

        let _handle = tokio::spawn(async move { 
            let mut node = Node::new(i, reciever, peers);
            node.recieve().await.unwrap();
        });

        i += 1;
    }

    // !! 
    let client_sleep_time: u64 = 2;

    // send some requests to broadcast
    let mut step = 0;
    for _ in 0..10 { 
        let node_index: usize = thread_rng().gen_range(0..n_nodes);
        let message = Message::ClientMessage(ClientMessage { data: step });
        let sender = &world_senders[node_index].1;

        info!("Client sending {step:?}...");
        sender.send(message).await.unwrap();

        step += 1;
        sleep(Duration::from_secs(client_sleep_time)).await;
    }

}

mod tests { 
    #[test]
    pub fn test_convergence() -> anyhow::Result<()> { 
         

        Ok(())
    }

}
