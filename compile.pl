#! /usr/bin/perl

system "mvn clean package";
chdir "./src";
system "mvn package -PotherOutputDir";
chdir '../'
# chdir "./main/jni_fpga";
# system "mvn package -PotherOutputDir";
# chdir "../alphadata";
# system "sdaccel alphadata_host.tcl";
# chdir "../../../";
