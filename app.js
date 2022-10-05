/*
 * --------------------------------------------------------------------------- *
 * File: index.js                                                              *
 * Project: redisblog                                                          *
 * Created Date: 03 Oct 2022                                                   *
 * Author: Vikas K Solegaonkar (vikas@crystalcloudsolutions.com)               *
 * Copyright (c) 2022 Vikas K Solegaonkar                                      *
 * Crystal Cloud Solutions (https://crystalcloudsolutions.com)                 *
 *                                                                             *
 * Last Modified: Mon Oct 03 2022                                              *
 * Modified By: Vikas K Solegaonkar                                            *
 *                                                                             *
 * HISTORY:                                                                    *
 * --------------------------------------------------------------------------- *
 * Date         By     Comments                                                *
 * --------------------------------------------------------------------------- *
 */

const express = require("express");
const app = express();
const redis = require("redis");
const axios = require("axios");
const md5 = require("md5");
const config = require("config");

const setup = async () => {
  const client = redis.createClient(config.redis);
  client.on("error", (err) => console.log("Redis Client Error", err));
  await client
    .connect()
    .then((e) => console.log("Connected"))
    .catch((e) => console.log("Not connected"));

  await client.flushDb();

  var data = await axios.get(config.dataSource);
  console.log(data.data.source);
  var list = data.data.list;
  var promiseList = list.map((poem, i) =>
    Promise.all(Object.keys(poem).map((key) => client.hSet(`poem:${md5(i)}`, key, poem[key])))
  );
  await Promise.all(promiseList);

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

  app.get("/author/:author", function (req, res) {
    client.ft.search("idx:poems", `@author: /${req.params.author}/`).then((result) => res.send(result.documents));
  });

  app.get("/title/:title", function (req, res) {
    client.ft.search("idx:poems", `@title: /${req.params.title}/`).then((result) => res.send(result.documents));
  });

  app.get("/type/:type", function (req, res) {
    client.ft.search("idx:poems", `@title: ${req.params.type}`).then((result) => res.send(result.documents));
  });

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

setup().then((x) => console.log("Ready"));
