/*
 * --------------------------------------------------------------------------- *
 * File: hello_async_await.js                                                  *
 * Project: redisblog                                                          *
 * Created Date: 01 Oct 2022                                                   *
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

const redis = require("redis");
const axios = require("axios");
const md5 = require("md5");
const config = require("config");

const runSetup = async () => {
  var data = await axios.get(config.dataSource);
  console.log(data.data.source);
  var list = data.data.list;

  const client = redis.createClient(config.redis);
  client.on("error", (err) => console.log("Redis Client Error", err));
  await client.connect();

  var promiseList = list.map((poem, i) =>
    Promise.all(Object.keys(poem).map((key) => client.hSet(`poem:${md5(i)}`, key, poem[key])))
  );
  await Promise.all(promiseList);

  client.quit();
};

try {
  runSetup();
} catch (e) {
  console.log(e);
}
