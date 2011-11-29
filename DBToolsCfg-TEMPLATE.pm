package DBToolsCfg;
use warnings;
use strict;
use Readonly;
use base qw(Exporter);
our @EXPORT_OK = qw( 
    $DEFAULT_USER
    $DEFAULT_PASS
    $LOG_FILE
);
our %EXPORT_TAGS = (
    credentials   => [ qw(
        $DEFAULT_USER
        $DEFAULT_PASS
    )],
);

##
## Change the vars below and rename this file to DBToolsCfg.pm
##

Readonly our $DEFAULT_USER     => q{YourName};
Readonly our $DEFAULT_PASS     => q{YourPass};
Readonly our $LOG_FILE => q{/Users/ingolantschner/Library/Scripts/absql/logs/DBTools.log}; 
