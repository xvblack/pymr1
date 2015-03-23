struct FileHandle{
	1:string file_id;
	2:i32 loc;
}

struct ReadResult{
	1:FileHandle handle;
	2:bool EOF;
	3:binary data;
}

service FileSystem{
	FileHandle open(1:string file_id);
	ReadResult read(1:FileHandle handle, 2:i32 size);
	void close(1:FileHandle handle);
}