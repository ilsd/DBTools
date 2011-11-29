#!/usr/local/bin/perl -w
use warnings;
use strict;
use Carp; use Carp::Heavy;
use feature ":5.10";

use Test::More 'no_plan'; #tests => 13;
use Test::NoWarnings;
use Test::Exception;
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

can_ok('DBTools', 'get_id');
is( $db->get_id(), undef, 'w/ no args returns undef' );
is( $db->get_id(val => 'xyz'), undef, 'non-matching value returns undef' );
is( $db->get_id(col => 'name', val => 'Testorg'), 0, 'Testorg has id 0' );
is( $db->get_id(col => 'name', val => 'Testorg', col_to_return => 'uid'), 'TEST000000', 'Testorg has uid TEST000000' );
is( $db->get_id(col => 'name', val => 'Testorg 1'), 1, 'Testorg 1 has id 1' );
is( $db->get_id(col => 'cc', val => 'cct1'), 1, 'Testorg 1 with cc=cct1 has id 1' );
is( $db->get_id(col => 'cc', val => 'cct2'), 2, 'Testorg 2 with cc=cct2 has id 2' );

can_ok('DBTools', 'insert_row');
my $table_name = 'organisations';
my %data = (
    id => 999999,
    name => 'unit-test-310-01',
    uid => 'UT001',
    cc => 'ut01',
);

is ($db->insert_row("$table_name", \%data), $data{'id'}, "insert returns id" );
is ($db->get_id(col => 'name', val => $data{'name'}, col_to_return => 'uid'), $data{'uid'}, 'uid matches' );
is ($db->delete_row(table => "$table_name", val => '9999909'), '0E0', "deleting not-existing row returns 0" );
is ($db->delete_row(table => "$table_name", val => $data{'id'}), 1, "deleting one row returns 1" );

my %data01 = (
    id => 999901,
    name => 'unit-test-310-same_name',
    uid => 'UT9901',
    cc => 'ut991',
);

my %data02 = (
    id => 999902,
    name => 'unit-test-310-same_name',
    uid => 'UT9902',
    cc => 'ut992',
);

# create two rows with the same name
is ($db->insert_row("$table_name", \%data01), $data01{'id'}, "insert returns id" );
is ($db->insert_row("$table_name", \%data02), $data02{'id'}, "insert returns id" );

is( $db->get_id(col => 'name', val => $data01{'name'}), $data01{'id'}, 'returns id of the first entry' );
isnt( $db->get_id(col => 'name', val => $data02{'name'}), $data02{'id'}, 'does NOT return id of other matching rows' );

is ($db->delete_row(table => "$table_name", val => $data01{'id'}), 1, "first row sucessfully deleted" );
is ($db->delete_row(table => "$table_name", val => $data02{'id'}), 1, "second row sucessfully deleted" );

__END__
