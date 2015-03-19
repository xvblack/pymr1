

struct Location{
	1:string host,
	2:string port
}

typedef map<string, string> configuration;
typedef map<string, string> information;

service MapRedMaster{
	void run_task(1:configuration conf, 2:binary zip);
	Location get_map_output_locations(1:i32 reduce_id);

	void report_progress(1:configuration map_conf, 2:information progress_info);
	void register_output(1:configuration map_conf, 2:i32 reduce_id, 3:information output_info);
	void mark_finished_output(1:configuration map_conf);
}