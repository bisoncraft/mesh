package main

type Config struct {
	AppDataDir string `short:"A" long:"appdata" description:"Path to application home directory."`
	ConfigFile string `short:"C" long:"configfile" description:"Path to configuration file."`
	DebugLevel string `short:"d" long:"debuglevel" description:"Logging level {trace, debug, info, warn, error, critical}."`
}
