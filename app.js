import fetch from "node-fetch";
import { createGunzip } from "zlib";
import { MongoClient } from "mongodb";
import readline from "readline";

const url = "https://popwatch-staging.s3.us-east-2.amazonaws.com/movies_1.gz";
const uri = "mongodb://localhost:27017";
const dbName = "moviesDB";
const collectionName = "movies";

const client = new MongoClient(uri, { useUnifiedTopology: true });

fetch(url)
    .then((res) => {
        const gunzip = createGunzip();
        const stream = res.body.pipe(gunzip);
        const rl = readline.createInterface({ input: stream });

        const movies = [];

        rl.on("line", (line) => {
            const movie = JSON.parse(line);
            movies.push(movie);
        });

        rl.on("close", () => {
            client.connect((err) => {
                if (err) throw err;
                const db = client.db(dbName);
                const collection = db.collection(collectionName);

                collection
                    .insertMany(movies)
                    .then((res) => {
                        console.log(
                            `Inserted ${movies.length} documents into ${collectionName}`
                        );
                        client.close();
                    })
                    .catch((err) => {
                        console.error(err);
                        client.close();
                    });
            });
        });
    })
    .catch((err) => console.error(err));