


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