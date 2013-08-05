#!/usr/bin/env node
var fs = require('fs'),
    attrs = require('./0_Addresses.json'),
    ids = attrs.features.map(function(f) { return parseInt(f.attributes.BRT_ID);});

fs.writeFileSync('opaid_list.json', JSON.stringify(ids));
