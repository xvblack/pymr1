struct Location{
	1:string host,
	2:string port
}

typedef map<string, string> configuration;
typedef map<string, string> information;

service Container{
	void run_task(1:configuration conf, 2:binary zip);
}