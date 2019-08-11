package settings

// Soft is the soft settings that can be changed after the deployment of a
// system.
var Soft = getSoftSettings()

type soft struct {
	StreamConnections uint64
	LazyFreeCycle     uint64
}

func getSoftSettings() soft {
	org := getDefaultSoftSettings()
	return org
}

func getDefaultSoftSettings() soft {
	return soft{
		StreamConnections: 4,
		LazyFreeCycle:     1,
	}
}
