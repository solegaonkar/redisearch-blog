/*
 * --------------------------------------------------------------------------- *
 * File: index.js                                                              *
 * Project: redisblog                                                          *
 * Created Date: 03 Oct 2022                                                   *
 * Author: Vikas K Solegaonkar (vikas@crystalcloudsolutions.com)               *
 * Copyright (c) 2022 Vikas K Solegaonkar                                      *
 * Crystal Cloud Solutions (https://crystalcloudsolutions.com)                 *
 *                                                                             *
 * Last Modified: Wed Oct 05 2022                                              *
 * Modified By: Vikas K Solegaonkar                                            *
 *                                                                             *
 * HISTORY:                                                                    *
 * --------------------------------------------------------------------------- *
 * Date         By     Comments                                                *
 * --------------------------------------------------------------------------- *
 */

const express = require("express");
const redis = require("redis");
const axios = require("axios");
const md5 = require("md5");
const config = require("config");

/**
 * Setup the database.
 * 1. Pull the poems data dump from the provided URL
 * 2. Save the individual JSON records into hashes.
 * 3. Create a search index on the data available
 */
const setup = async () => {
  /**
   * Create the client object, pass in the credentials
   */
  const client = redis.createClient(config.redis);
  client.on("error", (err) => console.log("Redis Client Error", err));
  await client
    .connect()
    .then((e) => console.log("Connected"))
    .catch((e) => console.log("Not connected"));

  /**
   * Cleanup the database and remove any data already present there.
   */
  await client.flushDb();

  /**
   * Download the poems data dump from the URL provided
   */
  var data = await axios.get(config.dataSource);
  console.log(data.data.source);
  var list = data.data.list;
  /**
   * Save each individual poem as a Hash
   */
  var promiseList = list.map((poem, i) =>
    Promise.all(Object.keys(poem).map((key) => client.hSet(`poem:${md5(i)}`, key, poem[key])))
  );
  await Promise.all(promiseList);

  /**
   * Create the inverted index that we will use to query the poems data
   */
  await client.ft.create(
    "idx:poems",
    {
      content: redis.SchemaFieldTypes.TEXT,
      author: redis.SchemaFieldTypes.TEXT,
      title: { type: redis.SchemaFieldTypes.TEXT, sortable: true },
      type: redis.SchemaFieldTypes.TAG,
      age: redis.SchemaFieldTypes.TAG,
      type: redis.SchemaFieldTypes.TAG,
    },
    {
      ON: "HASH",
      PREFIX: "poem:",
    }
  );
  return client;
};

/**
 * Once we have the correct data in the database and the client is ready as well, we can start with
 * building the Express server.
 *
 * @param {*} client
 */
const createApp = (client) => {
  const app = express();
  /**
   * This creates an API that searches data by the author of poems. Note that it is a reqular expression match
   */
  app.get("/author/:author", function (req, res) {
    client.ft.search("idx:poems", `@author: /${req.params.author}/`).then((result) => res.send(result.documents));
  });

  /**
   * This creates an API that searches data by the title of poems. Note that it is a reqular expression match
   */
  app.get("/title/:title", function (req, res) {
    client.ft.search("idx:poems", `@title: /${req.params.title}/`).then((result) => res.send(result.documents));
  });

  /**
   * This creates an API that searches data by the type of poems. Note that it is an exact match
   */
  app.get("/type/:type", function (req, res) {
    client.ft.search("idx:poems", `@type: ${req.params.type}`).then((result) => res.send(result.documents));
  });

  /**
   * This creates an API that searches data by the type of poems. Note that it is an exact match
   */
  app.get("/fuzzy/:text", function (req, res) {
    client.ft.search("idx:poems", `%${req.params.text}%`).then((result) => res.send(result.documents));
  });

  /**
   * This API demonstrates the GROUPBY. It will group the results based on the field name specified.
   */
  app.get("/groupcounts/:field", function (req, res) {
    client.ft
      .aggregate("idx:poems", "*", {
        STEPS: [
          {
            type: redis.AggregateSteps.GROUPBY,
            properties: { property: req.params.field },
            REDUCE: [
              {
                type: redis.AggregateGroupByReducers.COUNT,
                property: req.params.field,
                AS: `count-${req.params.field}`,
              },
            ],
          },
        ],
      })
      .then((result) => res.send(result.results))
      .catch((e) => console.log(e));
  });

  app.listen(3000);
};

setup().then(createApp);
