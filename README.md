# rrdagg - a Scala program to aggregate the data from multiple rrd files

This is a relatively simple program which can combine data from multiple rrd files
(produced by [rrdtool](http://oss.oetiker.ch/rrdtool/)) using one of the supported
operations - currently SUM, AVG and SUMN where the last one is same as SUM but
treating NaN as 0.0.

Files must be compatible which currently means same number of data sources and same
RRA definitions.

The first file in the specified list will be used as "template" and the resulting rrd
file will have the same structure and period covered.

Internally rrdagg works by outputting xml dumps from each of the source files and
parsing these. Then for each rra and each data point within (based on the first source)
it will produce a single data point from the aggregated sources. The result is then
dumped as a XML and then restored by using rrdtool.

## Build

    $ sbt assembly

On success that will output a file named target/scala-2.12/rrdagg-assembly-1.0.jar

## Usage

    <prog-name> [-r|--rrdtool </path/to/rrdtool>] <op=SUM|AVG|SUMN> <target.rrd> <source1.rrd> <source2.rrd>...

<prog-name> can be java -jar path/to/rrdagg-assembly.jar or a batch file wrapping that.

If the first argument after the program name starts with a dash it will be treated as an
option. Currently only one option is supported and that is a path to rrdtool executable
after an -r or --rrdtool. If that option is not specified rrdtool is expected to be
on the PATH.

The first non-option argument is the aggregate operation and must be one of SUM, AVG or SUMN.

The second non-option argument is the target rrd file we want to output. Note that rrdagg will
fail on the output step if this file already exists.

The remaining arguments are treated as source files. At least two source files must be
provided.

Example

    $ java -jar target/scala-2.12/rrdagg-assembly-1.0.jar SUMN test_data/out2.rrd test_data/rrd1.rrd test_data/rrd2.rrd

## Known limitations (TODO)

- the last data points are currently not merged properly. This might result in some small 
gaps if one tries to continue updates on the aggregated rrd file. Fix for this is TBD.

- currently only rrdtool dump version 003 is tested/supported.
