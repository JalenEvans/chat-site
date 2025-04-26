import express from 'express';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { availableParallelism } from 'node:os';
import cluster from 'node:cluster';
import { createAdapter, setupPrimary  } from '@socket.io/cluster-adapter';

// open the database file
const db = await open({
  filename: 'chat.db',
  driver: sqlite3.Database
})

await db.exec('CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, client_offset TEXT UNIQUE, content TEXT);');

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  // create one worker for available core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i,
    });
  }

  setupPrimary();
}
else {
  const app = express();
  app.use(express.static('public'));
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    // set up an adapter for each worker thread
    adapter: createAdapter(),
  });

  const __dirname = dirname(fileURLToPath(import.meta.url));

  app.get('/', (req, res) => {
    res.sendFile(join(__dirname, 'index.html'));
  });

  io.on('connection', async (socket) => {
    console.log('a user connected');

    socket.on('disconnect', () => {
      console.log('a user disconnected');
    })

    socket.on('chat message', async (msg, clientOffset, callback) => {
      let result;
      try {
        result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', msg, clientOffset);
      } catch (e) {
        if (e.errno === 19) {
          callback();
        }
        else {

        }
        return;
      }

      io.emit('chat message', msg, result.lastID);
      callback();
    })

    if (!socket.recovered) {
      try {
        await db.each('SELECT id, content FROM messages WHERE id > ?',
          [socket.handshake.auth.serverOffset || 0],
          (err, row) => {
            socket.emit('chat message', row.content, row.id);
          }
        )
      }
      catch (e) {
        console.log(e);
      }
    }
  })

  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`)
  });
}