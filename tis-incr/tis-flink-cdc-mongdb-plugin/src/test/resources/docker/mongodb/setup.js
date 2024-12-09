// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// In production you would almost certainly limit the replication user must be on the follower (slave) machine,
// to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
// However, in this database we'll grant flink user with privileges:
//
// 'flinkuser' - all privileges required by the snapshot reader AND stream reader (used for testing)
//

//use admin;
db.createRole(
    {
        role: "flinkrole",
        privileges: [{
            // Grant privileges on All Non-System Collections in All Databases
            resource: { db: "", collection: "" },
            actions: [ "splitVector", "listDatabases", "listCollections", "collStats", "find", "changeStream" ]
        }],
        roles: [
            { role: 'read', db: 'config' }
        ]
    }
);

db.createUser(
 {
   user: 'flinkuser',
   pwd: 'a1?~!@#$%^&*(){}[]<>.,+_-=/|:;',
   roles: [
      { role: 'flinkrole', db: 'admin' }
   ]
 }
);