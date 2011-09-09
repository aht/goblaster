include $(GOROOT)/src/Make.inc

TARG=blaster
GOFILES=\
	blaster.go\
	echotcpblaster.go

include $(GOROOT)/src/Make.pkg
