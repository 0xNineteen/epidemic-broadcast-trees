use std::{time::{Duration, SystemTime}, sync::{Arc, atomic::{AtomicBool, Ordering}}, collections::HashMap, thread};

use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};
use sha2::Sha256;
use sha2::Digest;
use tokio::{sync::{mpsc::{self, Sender, Receiver}, RwLock}, time::{sleep, interval}};
use tracing::{info, debug};
use rand::{self, Rng, thread_rng};

pub const HASH_BYTE_SIZE: usize = 4;
pub type Sha256Bytes = [u8; HASH_BYTE_SIZE];
pub const FANOUT_SIZE: usize = 2;
type Peer = (usize, Arc<ChannelSender<Message>>);

macro_rules! node_log {
    ($data:expr) => {
        info!("NODE {:?}: {}...", std::thread::current().name(), $data);
    };
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Copy)]
pub enum Message { 
    ClientMessage(ClientMessage), 
    EagerMessage(BodyMessage), 
    // LazyMessage(HeaderMessage), // note: solana doesnt do this 
    PullMessage(HeaderMessage),  // solana does this periodically with a bloom filter 
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

pub struct ChannelReciever<T>(Receiver<T>);

pub struct ChannelSender<T>(Sender<T>);

trait NetworkReciever<T> { 
    fn try_recv(&mut self) -> Option<T>; 
}

trait NetworkSender<T> { 
    fn send(&self, data: T) -> Result<()>;
}

impl<T> NetworkReciever<T> for ChannelReciever<T> { 
    fn try_recv(&mut self) -> Option<T> {
        self.0.try_recv().map(Some).unwrap_or(None)
    }
}

impl<T> NetworkSender<T> for ChannelSender<T> { 
    fn send(&self, data: T) -> Result<()> {
        self.0.try_send(data).map_err(|_| anyhow!("sending error: ..."))
    }
}

pub struct Node { 
    pub id: usize,
    pub reciever: ChannelReciever<Message>,
    pub active_set: Vec<usize>,
    pub nodes: Vec<usize>, // indexs into the table Vec<node_id>
    pub table: HashMap<usize, Arc<ChannelSender<Message>>>, // node => sender
    pub exit: Arc<AtomicBool>
}

impl Node {
    pub fn new(id: usize, reciever: ChannelReciever<Message>, exit: Arc<AtomicBool>) -> Self { 
        Node { 
            id, 
            reciever, 
            active_set: vec![], 
            nodes: vec![], 
            table: HashMap::default(), 
            exit,
        }
    }

    pub fn add_peers(&mut self, peers: Vec<Peer>) { 
        let mut n_actives = FANOUT_SIZE - self.active_set.len();

        for peer in peers { 
            let id = peer.0; 
            let sender = peer.1; 

            self.nodes.push(id); 
            self.table.insert(id, sender);

            if n_actives != 0 { 
                self.active_set.push(id);
                n_actives -= 1;
            }
        }
    }
    
    pub fn handle_client_msg(&mut self, msg: ClientMessage) { 
        let time = std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
        let mut hasher = Sha256::new();
        hasher.update(time.to_le_bytes());
        hasher.update(msg.data.to_le_bytes());
        let msg_id = hasher.finalize().as_slice()[..4].try_into().unwrap();

        let header = HeaderMessage { sender_id: self.id, msg_id }; 
        let msg = Message::EagerMessage(
            BodyMessage { data: msg.data, header }
        );

        // broadcast msg to active set
        for peer_id in &self.active_set { 
            let sender = self.table.get(&peer_id).unwrap();
            match sender.send(msg) { 
                Err(e) => {
                    // todo: track metrics
                    node_log!(format!("msg send failed: {:?}", e));
                }
                _ => {}
            }
        }
    }

    pub fn handle_eager_msg(&mut self, msg: BodyMessage) { 

    }

    pub fn handle_pull_msg(&mut self, msg: HeaderMessage) { 

    }

    pub fn handle_prune_msg(&mut self, msg: IDMessage) { 

    }

    pub fn reciever(&mut self) { 
        loop { 
            if let Some(msg) = self.reciever.try_recv() {
                match msg { 
                    Message::ClientMessage(msg) => { 
                        self.handle_client_msg(msg);
                    }, 
                    Message::EagerMessage(msg) => { 
                        self.handle_eager_msg(msg);
                    }, 
                    Message::PullMessage(msg) => { 
                        self.handle_pull_msg(msg);
                    }, 
                    Message::PruneMessage(msg) => { 
                        self.handle_prune_msg(msg);
                    }, 
                }
            }

            if self.exit.load(Ordering::Relaxed) { 
                break;
            }

            thread::sleep(Duration::from_millis(500));
        }
    }
}

pub fn main() { 
    tracing_subscriber::fmt::init();

    let n_nodes = 4; 

    let exit = Arc::new(AtomicBool::new(false));
    let world = (0..n_nodes).map(|_| mpsc::channel::<Message>(100)).collect::<Vec<_>>();
    let nodes = world
        .into_iter()
        .enumerate()
        .map(|(id, (sender, reciever))| { 
            let sender = (id, Arc::new(ChannelSender(sender)));
            let reciever = ChannelReciever(reciever);
            let node = Node::new(id, reciever, exit.clone());
            (node, sender)
        })
        .collect::<Vec<_>>();
    
    let peers = nodes
        .iter()
        .map(|(_, sender)| sender.clone())
        .collect::<Vec<_>>();

    let mut handles = vec![];
    for (mut node, _) in nodes { 
        node.add_peers(peers.clone());
        let handle = std::thread::spawn(move || { 
            node.reciever();
        });
        handles.push(handle);
    }

    for handle in handles { 
        let _ = handle.join();
    }
}

#[cfg(test)]
pub mod tests { 
    use super::*;

    pub fn new_node(id: usize) -> (Node, ChannelSender<Message>) { 
        let exit = Arc::new(AtomicBool::new(false));
        let (sender, reciever) = mpsc::channel::<Message>(100); 
        let reciever = ChannelReciever(reciever);
        let sender = ChannelSender(sender);
        let node = Node::new(id, reciever, exit);
        (node, sender)
    }

    #[test]
    pub fn test_new_peer() { 
        let (mut node, _) = new_node(0);

        let id = 2; 
        let (sender, _) = mpsc::channel::<Message>(100); 
        let peer = (id, Arc::new(ChannelSender(sender)));
        node.add_peers(vec![peer]); 

        assert_eq!(node.active_set.len(), 1); 
        assert_eq!(node.nodes.len(), 1); 
        assert_eq!(node.nodes[0], id); 
        assert!(node.table.get(&id).is_some()); 
    }

    #[test]
    pub fn test_exit() { 
        let (mut node, _) = new_node(0);

        let exit = node.exit.clone();
        let handle = std::thread::spawn(move || { 
            node.reciever();
        });
        exit.store(true, Ordering::Relaxed);

        let _ = handle.join();
    }

    #[test]
    pub fn test_client_broadcast() { 
        let (mut node, sender) = new_node(0);

        // add a peer
        let id = 2; 
        let (peer_sender, mut reciever) = mpsc::channel::<Message>(100); 
        let peer = (id, Arc::new(ChannelSender(peer_sender)));
        node.add_peers(vec![peer]); 

        let exit = node.exit.clone();
        let handle = std::thread::spawn(move || { 
            node.reciever();
        });

        // send a client msg
        let msg_data = 0;
        let msg = Message::ClientMessage(ClientMessage { data: msg_data });
        sender.send(msg).unwrap();

        // peer should recieve it 
        thread::sleep(Duration::from_millis(10));
        let msg = reciever.try_recv();
        assert!(msg.is_ok());
        let msg = msg.unwrap();

        match msg {
            Message::EagerMessage(msg) => { 
                assert_eq!(msg.data, msg_data);
            }
            _ => assert!(false) 
        }

        exit.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }

}

// node sends = id, node_channel
// nodes.push(id)
// table.insert(id, node_channel)

// impl Node { 
//     pub fn new(id: usize, reciever: Receiver<Message>, peers: Vec<Arc<Peer>>) -> Self { 
//         // ! 
//         let peer_ids = peers.iter().map(|peer| peer.0).collect::<Vec<_>>();
    
//         // track peers
//         let n_nodes = peers.len();
    
//         // first eager (fanout size)
//         let mut eager_peers = vec![];
//         let mut eager_ids = Vec::with_capacity(FANOUT_SIZE);
//         for _ in 0..FANOUT_SIZE { 
//             // sample without replacement 
//             loop { 
//                 let node_index: usize = thread_rng().gen_range(0..n_nodes);
//                 let node_id = peer_ids[node_index];
    
//                 if !eager_ids.contains(&node_id) { 
//                     eager_ids.push(node_id);
//                     eager_peers.push(peers[node_index].clone());
//                     break;
//                 }
//             }
//         }
    
//         // lazy the rest
//         let lazy_peers = peers
//             .iter()
//             .filter(|peer| !eager_ids.contains(&peer.0))
//             .map(|peer| peer.clone())
//             .collect::<Vec<_>>();
//         let lazy_ids = lazy_peers.iter().map(|peer| peer.0).collect::<Vec<_>>();
    
//         info!("NODE {id:?}: lazy {:?} eager {:?} total_n {:?}", lazy_ids, eager_ids, peers.len());
//         assert!(lazy_peers.len() + eager_peers.len() == peers.len());
//         assert!(lazy_peers.len() == lazy_ids.len());
//         assert!(eager_peers.len() == eager_ids.len());

//         // track what messages it has recieved so far 
//         let msg_ids = vec![]; 
//         let msg_data = vec![];

//         Node { 
//             reciever, 
//             id, 
//             msg_data, 
//             msg_ids, 
//             eager_peers, 
//             eager_ids, 
//             lazy_peers, 
//             lazy_ids, 
//         }
//     }

//     pub fn move_to_lazy(&mut self, id: usize) -> Arc<Peer> { 
//         let peer = swap!(id, self.eager_peers, self.eager_ids => self.lazy_peers, self.lazy_ids)();
//         info!("NODE {:?}: moving {:?} to lazy...", self.id, id);
//         info!("NODE {:?}: => lazy {:?} eager {:?}", self.id, self.lazy_ids, self.eager_ids);
//         peer
//     }

//     pub fn move_to_eager(&mut self, id: usize) -> Arc<Peer> { 
//         let peer = swap!(id, self.lazy_peers, self.lazy_ids => self.eager_peers, self.eager_ids)();
//         info!("NODE {:?}: moving {:?} to eager...", self.id, id);
//         info!("NODE {:?}: => lazy {:?} eager {:?}", self.id, self.lazy_ids, self.eager_ids);
//         peer
//     }

//     pub async fn handle_eager_message(&mut self, msg: BodyMessage) -> Result<()> { 
//         let BodyMessage { 
//             header: HeaderMessage { sender_id, msg_id }, 
//             data
//         } = msg;

//         let is_eager_sender = self.eager_ids.contains(&sender_id);
//         if !self.msg_ids.contains(&msg_id) { 

//             // track message
//             self.msg_ids.push(msg_id);
//             self.msg_data.push(data);

//             info!("NODE {:?}: broadcasting...", self.id);
//             let header_msg = HeaderMessage { sender_id: self.id, msg_id}; 
//             let lazy_message = Message::LazyMessage(header_msg);
//             let eager_message = Message::EagerMessage(BodyMessage { data, header: header_msg});

//             // exclude the sender which it was recieved from 
//             broadcast_exclude(&self.eager_peers, eager_message, sender_id).await?;
//             broadcast_exclude(&self.lazy_peers, lazy_message, sender_id).await?;

//             if !is_eager_sender { 
//                 // move to eager
//                 self.move_to_eager(sender_id);
//             } 

//         } else if is_eager_sender { 
//             let peer = self.move_to_lazy(sender_id);
//             // send prune
//             let message = Message::PruneMessage(IDMessage { sender_id: self.id });
//             peer.1.send(message).await?;
//         }

//         Ok(())
//     }

//     pub async fn handle_pull_message(&mut self, msg: HeaderMessage) -> Result<()> { 
//         if !self.eager_ids.contains(&msg.sender_id) { 
//             // add new link to eager
//             self.move_to_eager(msg.sender_id);
//         } 
//         let HeaderMessage { msg_id: req_msg_id, .. } = msg;

//         if let Some(index) = self.msg_ids.iter().position(|msg_id| *msg_id == req_msg_id) { 
//             info!("NODE {:?}: replying to pull request...", self.id);
//             let data = self.msg_data[index];

//             // create the response
//             let header_msg = HeaderMessage { sender_id: self.id, msg_id: req_msg_id }; 
//             let message = Message::EagerMessage(BodyMessage { data, header: header_msg });
//             let source_peer = self.eager_peers.iter().find(|peer| peer.0 == msg.sender_id).unwrap();
//             source_peer.1.send(message).await?;
//         } else { 
//             panic!("NODE {:?}: pull request message not found - abort!", self.id);
//         }

//         Ok(())
//     }

//     pub async fn handle_prune_message(&mut self, msg: IDMessage) -> Result<()> { 
//         if self.eager_ids.contains(&msg.sender_id) { 
//             self.move_to_lazy(msg.sender_id);
//         }

//         Ok(())
//     }

//     pub async fn handle_client_message(&mut self, msg: ClientMessage) -> Result<()> { 
//         let time = std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
//         let mut hasher = Sha256::new();

//         hasher.update(time.to_le_bytes());
//         hasher.update(msg.data.to_le_bytes());
//         let msg_id = hasher.finalize().as_slice()[..4].try_into().unwrap();

//         self.msg_ids.push(msg_id);
//         self.msg_data.push(msg.data);

//         let header_message = HeaderMessage { sender_id: self.id, msg_id }; 
//         let eager_message = Message::EagerMessage(
//             BodyMessage { data: msg.data, header: header_message }
//         );
//         let lazy_message = Message::LazyMessage(
//             header_message
//         );

//         info!("NODE {:?}: broadcasting...", self.id);

//         // broadcast to eager 
//         broadcast(&self.eager_peers, eager_message).await?;
//         // broadcast to lazy 
//         broadcast(&self.lazy_peers, lazy_message).await?;

//         Ok(())
//     }

//     pub async fn recieve(&mut self) -> anyhow::Result<()> { 
//         let mut missing_messages: Vec<HeaderMessage> = vec![];
//         let mut check_tick = interval(Duration::from_secs(1000)); // placeholder
//         let mut state_tick = interval(Duration::from_secs(3)); 

//         // start
//         loop { 
//             tokio::select! {
//                 _ = state_tick.tick() => { 
//                     info!("NODE {:?}: state = {:?}", self.id, self.msg_data.last());
//                 }
//                 _ = check_tick.tick() => { 
//                     if let Some(mut msg) = missing_messages.pop() { 
//                         if !self.msg_ids.contains(&msg.msg_id) { 
//                             // todo: remove the peer which never eager sent
//                             // need a new eager path bc we didnt get the data
//                             let message = Message::PullMessage(HeaderMessage { sender_id: self.id, msg_id: msg.msg_id });
//                             let source_peer = {
//                                 let peer = self.lazy_peers.iter().find(|peer| peer.0 == msg.sender_id);
//                                 if peer.is_none() { 
//                                     self.eager_peers.iter().find(|peer| peer.0 == msg.sender_id).unwrap()
//                                 } else { 
//                                     peer.unwrap()
//                                 }
//                             };
//                             source_peer.1.send(message).await?;
                            
//                             if !self.eager_ids.contains(&msg.sender_id) { 
//                                 // add new link to eager
//                                 self.move_to_eager(msg.sender_id);
//                             } 
                            
//                             // change the sender id to a new random node (in case this one fails too)
//                             let node_index: usize = thread_rng().gen_range(0..self.lazy_ids.len());
//                             msg.sender_id = self.lazy_ids[node_index];
//                             missing_messages.push(msg);

//                         } else { 
//                             info!("NODE {:?}: pull request succeeded!", self.id);
//                         }
//                     }

//                     if missing_messages.len() == 0 { 
//                         // reset tick
//                         check_tick = interval(Duration::from_secs(1000)); // placeholder
//                     }
//                 }
//                 resp = self.reciever.recv() => { 
//                     if resp.is_none() { return Ok(()) }
//                     let msg = resp.unwrap();
//                     info!("NODE {:?}: recieved: {msg:?}", self.id);

//                     match msg { 
//                         Message::ClientMessage(msg) => { self.handle_client_message(msg).await? }
//                         Message::EagerMessage(msg) => { self.handle_eager_message(msg).await? }
//                         Message::LazyMessage(msg) => {
//                             // wait for missing message
//                             missing_messages.push(msg);
//                             let sleep_time = Duration::from_secs(2);
//                             check_tick = interval(sleep_time);
//                             let _ = check_tick.tick().await; // scratch first tick
//                         }
//                         Message::PullMessage(msg) => { self.handle_pull_message(msg).await? }
//                         Message::PruneMessage(msg) => { self.handle_prune_message(msg).await? }
//                     }
//                 }
//             }
//         }
//     }
// }

// pub async fn broadcast_exclude(peers: &Vec<Arc<Peer>>, msg: Message, exclude_id: usize) -> Result<()> { 
//     for peer in peers { 
//         if peer.0 != exclude_id { 
//             peer.1.send(msg).await?; // todo: move await out of loop
//         }
//     }

//     Ok(())
// }

// pub async fn broadcast(peers: &Vec<Arc<Peer>>, msg: Message) -> Result<()> { 
//     for peer in peers { 
//         peer.1.send(msg).await?; // todo: move await out of loop
//     }

//     Ok(())
// }

// // pub async fn setup_world(n_nodes: usize) -> (Vec<Node>, Vec<Peer>) {
// //     (world, world_senders)
// // }

// #[tokio::main(flavor="multi_thread", worker_threads=16)]
// pub async fn main() { 
//     tracing_subscriber::fmt::init();

//     // !!
//     let n_nodes = 4; 

//     let world = (0..n_nodes).map(|_| mpsc::channel::<Message>(100)).collect::<Vec<_>>();
//     let world_senders = world.iter().enumerate().map(|(id, (sender, _))| Arc::new((id, sender.clone()))).collect::<Vec<_>>();

//     // setup every node
//     let mut i = 0;
//     for (_, reciever) in world {
//         let mut peers = world_senders.clone();
//         peers.remove(i);

//         let mut node = Arc::new(RwLock::new(Node::new(i, reciever, peers)));
        
//         let _handle = tokio::spawn(async move { 
//             let node = node.clone();
//             node.recieve().await.unwrap();
//         });

//         i += 1;
//     }

//     // !! 
//     let client_sleep_time: u64 = 2;

//     // send some requests to broadcast
//     let mut step = 0;
//     for _ in 0..10 { 
//         let node_index: usize = thread_rng().gen_range(0..n_nodes);
//         let message = Message::ClientMessage(ClientMessage { data: step });
//         let sender = &world_senders[node_index].1;

//         info!("Client sending {step:?}...");
//         sender.send(message).await.unwrap();

//         step += 1;
//         sleep(Duration::from_secs(client_sleep_time)).await;
//     }

// }

// mod tests { 

//     #[test]
//     pub fn test_convergence() -> anyhow::Result<()> { 
//         Ok(())
//     }

// }
