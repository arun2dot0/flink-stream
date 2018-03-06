package flinkforward;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * <p>You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-stream-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * 		./bin/flink run -c flinkforward.StreamingJob target/flink-stream-1.0-SNAPSHOT.jar
 *
 * <p>For more information on the CLI see:
 *
 * <p>http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		
		//Hello World
		DataStream<String> dataStream=null;
		
	    final ParameterTool params = ParameterTool.fromArgs(args);
	    
	    env.getConfig().setGlobalJobParameters(params);
	    
	    if(params.has("input"))
	    {
	    	dataStream = env.readTextFile(params.get("input"));
	    }
	    else if  (params.has("host")&& params.has("port"))
	    {
	    	dataStream =env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
	    }
	    else
	    {
	    	System.out.println("Use --host --port to specify socket");
	    	System.out.println("Use --input to specify file input");
	    }
	    
	    if(dataStream == null)
	    {
	    	System.exit(1);
	    	return;
	    }
	    
	    dataStream.print();
	    
	    dataStream.writeAsText(params.get("output"),FileSystem.WriteMode.OVERWRITE);
	    env.execute("Read and Write");
	    
		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		//env.execute("Flink Streaming Java API Skeleton");
	    
	    /* execute
	     * 
	     * flink run -c flinkforward.StreamingJob target/flink-stream-1.0-SNAPSHOT.jar --input ~/java/flink/flink-stream/flinkData/specialties.txt --output ~/java/flink/flink-stream/flinkOutputs/outFromTextFile.csv
		*flink run -c flinkforward.StreamingJob target/flink-stream-1.0-SNAPSHOT.jar --host localhost --port 9000 --output ~/java/flink/flink-stream/flinkOutputs/outFromSocket.csv
		* execute nc -l 9000  , in other window as source for socket 
		* tail -f flink-*-taskmanager-*.out  , to see the  changes in log file simultaneously
		*/
	     
	}
}
