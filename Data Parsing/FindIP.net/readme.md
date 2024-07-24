FindIP_1.py :
This is able to process files found in a directory with the specified head name. It is able to process multiple files sequentially, which is great if the server running can have a 100% uptime wihtout worrying about it shutting down on its own.

FindIP_2.py :
This is able to process only a single file, which is made with the scenario if the server does not have a 100% uptime.

FindIP_3.py :
Specify an amount to query
Specify the max queries before it stops running.
This code is the most useful if you are running the code in an environment which has bad connections, no guarenteed uptime. This code processes chunks at each time and then append them to the csv file immediately, which solves the problem of the code queries being lost due to unexpected circumstances.
