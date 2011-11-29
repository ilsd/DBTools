#!/usr/local/bin/perl -w
use warnings;
use strict;
use Carp; use Carp::Heavy;
use feature ":5.10";

use Test::More 'no_plan'; #tests => 13;
use Test::NoWarnings;
use Test::Exception;
use Test::Deep;
use Readonly;

# Constants
our $DEBUG = 0;
our $LOGG = 0;

# my modules
use FindBin qw($Bin);
use lib "$Bin/../scripts";

use_ok('DBTools');

# Constants
Readonly my $db_name  => 'DBTools_unit-tests';
Readonly my $db_user  => 'unit_tests';
Readonly my $db_pass  => 'hello';
Readonly my $table  => 'organisations';

can_ok('DBTools', 'new');
my $db = DBTools->new(db_name => $db_name, db_user  => $db_user, db_pass => $db_pass);

can_ok('DBTools', 'has_column');

is($db->has_column(table => $table, col => 'uid'), 1, 'Column does exist') ;
is($db->has_column(table => $table, col => 'thisNot'), undef, 'Column does not exist') ;

__END__