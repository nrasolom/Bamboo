#!/bin/bash

usage () {
	echo "" >&2
	echo "pandapush usage:" >&2
	echo "$ ppush -t <directory> [-p <path>] " >&2
	echo "" >&2
	echo "This script load a folder with a important number of files" >&2
	echo "in the hadoop filesystem. You need to be log as panda to use it" >&2
	echo "" >&2
	echo "param(s) :" >&2 
	echo "		Directory of files. It can be absolute or not, but it's mandatory" >&2
	echo "		Path to store the folder in the hadoop fs." >&2
	echo " 		It's a relative path and it start at /user/panda/" >&2
	echo "optioni(s) :" >&2
	echo "|         -t target folder to copie" >&2
	echo "| 	-p <path> path to the hadoop filesystem under /user/panda" >&2
	echo "| 	-v verbose mode" >&2
	echo "| 	-f force no confirmation" >&2
	echo "|         -h print this awsome helper message" >&2
	echo "" >&2
}

while getopts 't:p:vfh:' opt; do
	case $opt in
		t)
		  TARGET=$2
		  ;;
		p)
                  HDFS_PATH=$4
                  ;;
		f)
		  FORCE=true
		  ;;
		h)
                  usage
                  exit 0
                  ;;
		\?)
		  echo "Bad option -$OPTARG\n" >&2
      		  usage
      		  exit 1
		;;
	esac
done

# ### CHECK IF THE USER IS PANDA
if [ `whoami` == "panda" ]; then
	echo "Script start with 'panda' privileges."
else
	echo "You do not have the privileges 'panda'"
	echo "Script exit with error code: 1."
	exit 1
fi

# ### CHECK IF THE PARAM IS A DIRECTORY
if [ ! -d $TARGET ];then
	echo "$TARGET is not a directory."
	echo "Script exit with error code: 2."
	exit
fi   

if [ -z $FORCE ];then
	# ### CHECK THE NUMBER OF FILE IN THE FOLDER
	files=`find $TARGET -type f | wc -l`
	echo "$files file(s) from $TARGET will be put in the HDFS"

	read -p "Please, confirm this operation (y/n) " -r
	if [[ ! $REPLY =~ ^[Yy]$ ]]
	then
        	echo "Script exit with error code: 1."
        	exit 1
	fi
	
fi
	
echo "Start acces to the hadoop-client."
`hdfs dfs -put $TARGET /user/panda/` && echo "Script exit with error code: 2." | exit 2
echo "Script terminate with code: 1"
