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

can_ok('DBTools', 'get_row');

throws_ok { $db->get_row() } qr/missing argument 'table'/;
throws_ok { $db->get_row(table => 'organisations') } qr/missing argument 'val'/;
throws_ok { $db->get_row(val => '999') } qr/missing argument 'table'/;
is(${$db->get_row(table => 'organisations', val => '0')}{'name'}, q{Testorg}, 
    'name of id 0 is Testorg');
is(${$db->get_row(table => 'organisations', val => '1')}{'name'}, q{Testorg 1}, 
    'name of id 1 is Testorg 1');
is(${$db->get_row(table => 'organisations', val => '1')}{'uid'}, q{TEST000111}, 
    'uid of id 1 is TEST000111');

__END__