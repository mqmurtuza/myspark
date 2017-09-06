#!/usr/bin/expect

spawn gcloud dataproc clusters delete [lindex $argv 0]
expect "Do you want to continue (Y/n)?"
send "y\r"
interact

