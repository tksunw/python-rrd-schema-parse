#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 textwidth=79 autoindent

"""
Python source code
Last modified: 28 Feb 2011 - 18:13
Last author: Laban Mwangi

Dumps the schema of existing rrd files using rrdtool info,
Parses this dump, and generates an rrd graph definition 
"""
import optparse
import subprocess
import re
import sys


class RRDParser(object):

    """RRDParser - Given an rrd file, return it's graph definition
    Typically what you'd feed to rrdtool create to get the same graph
    Keyword
        Params - Dictionary
            rrdfile - String - File to read
            debug  - Int - Debug level >=0

    """
    def __init__(self, params):
        super(RRDParser, self).__init__()
        self.params = params
        self.info = ""
        self.schema = {}

    def _rrdinfodump(self):
        """rrdinfodump - Acquires the rrdinfo dump"""
        if self.params['debug'] >= 2:
            print "Getting dump for: %s" % (self.params['file'])
        
        cmd = ["rrdtool", "info",  self.params['file']]
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, close_fds=True)
        str_stdout = proc.stdout.read()
        str_stderr = proc.stdout.read()
        ret_code = proc.poll()

        if self.params['debug'] >= 2:
            print "command     : %s" % cmd
            print "Return Code : %s" % ret_code
            print "Stdout      : %s" % str_stdout
            print "Stderr      : %s" % str_stderr

        if str_stdout.find('header_size'):
            self.info = str_stdout
            return True
        else:
            return False
    
    def _parse_hdr(self):
        """_parse_hdr analyzes the info variable and updates hdr info."""
        self.schema["hdr"] = {}
        for line in self.info.split("\n"):
            for hdr in ["filename", "step", "last_update"]:
                if line.startswith(hdr):
                    self.schema["hdr"][hdr] = line.split(" = ")[-1].strip('"')

    def _parse_ds(self):
        """_parse_ds analyzes the info variable and updates the schema dict"""
        ds_re = r"ds\[(?P<datasource>.*?)\]\.(?P<field>.*?) "
        ds_re += r"= (?P<arg>.*)"
        ds_dict = {}

        for line in self.info.split("\n"):
            if line.startswith("ds"):
                m = re.search(ds_re, line)
                if m:
                    data_source, field, value =  m.groups(0)
                    if not data_source in ds_dict:
                        ds_dict[data_source] = {}

                    ds_dict[data_source][field] = value.strip('"').strip()

        if not len(ds_dict):
            print "DS is a required field"
            sys.exit(1)

        self.schema["ds"] = ds_dict

    def _parse_rra(self):
        """_parse_rra analyzes the info variable and updates the schema dict"""
        rra_re = r"rra\[(?P<datasource>\d\+?)\]\.(?P<field>[a-z_]+?) "
        rra_re += r"= (?P<arg>.*)"
        rra_dict = {}

        for line in self.info.split("\n"):
            if line.startswith("rra"):
                m = re.search(rra_re, line)
                if m:
                    rra, field, value =  m.groups(0)
                    if not rra in rra_dict:
                        rra_dict[rra] = {}

                    rra_dict[rra][field] = value.strip('"').strip()

        if not len(rra_dict):
            print "rra is a required field"
            sys.exit(1)

        self.schema["rra"] = rra_dict

    
    def parse(self):
        """Parse - Generates the new graph definition"""
        #if not self._rrdinfodump():
        #    print "Could not parse rrd file %s" % self.params['file']

        self._rrdinfodump()
        self._parse_hdr()
        self._parse_ds()
        self._parse_rra()

        hdr = self.schema["hdr"]
        ds = self.schema["ds"]
        rra = self.schema["rra"]

        print 'rrdtool create %(filename)s --start %(last_update)s' % hdr,
        print '--step %(step)s \\' % hdr
        
        for key,value in ds.iteritems():
            for item in ['max', 'min']:
                if value[item] == 'NaN':
                    value[item] = 'U'
            ds_str = '             DS:%s:' % key
            ds_str += '%s:%s:%s:%s \\' % (
                value['type'], value['minimal_heartbeat'],
                value['min'], value['max'])
            print ds_str

        rrakeys = rra.keys()
        rrakeys.sort()

        for key in rrakeys:
            value = rra[key]
            #print value
            for item in ['xff']:
                value[item] = float(value[item])
            rra_str = '             RRA:%s:%.1f:' % (value['cf'],
                                                     value['xff'])
            rra_str += '%s:%s \\' % (value['pdp_per_row'], value['rows'])
            print rra_str

#                            RRA:AVERAGE:0.5:1:10 """

        


def main():
    """Main function. Called when this file is a shell script"""
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage)

    parser.add_option("-v", "--verbose", dest="debug",
                      default="0", type="int",
                      help="Debug. Higher integers increases verbosity")

    parser.add_option("-f", "--file", dest="file",
                      default="test.rrd", type="string",
                      help="Existing rrdfile to parse")


    (options, args) = parser.parse_args()
    params = {}
    params['debug'] = options.debug
    params['file'] = options.file

    rrd_parser = RRDParser(params)
    rrd_parser.parse()
    #print rrd_parser.schema

if __name__ == '__main__':
    main()
