/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.lakefs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import static io.lakefs.contract.TestLakeFSFileSystemContract.TEST_FS_LAKEFS_NAME;


public class LakeFSContract extends AbstractBondedFSContract {

    public static final String CONTRACT_XML = "contract/lakefs.xml";


    public LakeFSContract(Configuration conf) {
        super(conf);
        //insert the base features
        addConfResource(CONTRACT_XML);
    }

    @Override
    public String getScheme() {
        return "lakefs";
    }

    @Override
    public Path getTestPath() {
        return new Path(getConf().get(TEST_FS_LAKEFS_NAME) + "/main/test/");
    }
}
