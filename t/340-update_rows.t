#!/usr/local/bin/perl -w
use warnings;
use strict;
use Carp; use Carp::Heavy;
use feature ":5.10";

use Test::More 'no_plan'; #tests => 13;
use Test::NoWarnings;
use Test::Warn;
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

can_ok('DBTools', 'update_rows');
can_ok('DBTools', 'insert_row');

$db->delete_row(
    table => "$table",
    col => 'id',
    val => 99934001,
);
$db->delete_row(
    table => "$table",
    col => 'id',
    val => 99934002,
);
$db->delete_row(
    table => "$table",
    col => 'id',
    val => 99934003,
);



$db->insert_row("$table", {id => 99934001, name => 'test3401', tel_nr => '123'});
$db->insert_row("$table", {id => 99934002, name => 'test3402', tel_nr => '123'});
$db->insert_row("$table", {id => 99934003, name => 'test3403', tel_nr => '123'});

$db->update_rows(
    table => $table, 
    data => {tel_nr  => 123001},
    where => {id => 99934001},
);

is(
    $db->get_id(
        table => "$table",
        col => 'id',
        val => 99934001,
        col_to_return => 'tel_nr',
    )
    ,123001
    , 'updated field matches'
);

warning_like
    {
        $db->update_rows(
            table => $table, 
            data => { tel_nr  => 9000 },
            where => { id => {'>', 0}},
            );
    } qr/Too many rows/i, q{Warns if more than one row affected};
    
is ( @{$db->get_ids(table => $table, where => {tel_nr => 9000})}, 0, 
    'data is unchanged' );
is ( @{$db->get_ids(table => $table, where => {tel_nr => 123001})}, 1, 
    'data is unchanged' );

$db->update_rows(
    table => $table, 
    data => {tel_nr  => 800},
    where => {id => {'>', 99900000}},
    max => 3,
);

is ( @{$db->get_ids(table => $table, where => {tel_nr => 800})}, 3, 
    'data is changed now' );

is( $db->update_rows(
        table => $table, 
        data => {tel_nr  => 700},
        where => {id => {'>', 99900000}},
        max => 3,
        dry_run => 1,
    ), 3, 'dry-run returns number of rows-affected'
);

is ( @{$db->get_ids(table => $table, where => {tel_nr => 700})}, 0, 
    'data has not been changed from dry-run' );
    
$db->delete_row(
    table => "$table",
    col => 'id',
    val => 99934001,
);
$db->delete_row(
    table => "$table",
    col => 'id',
    val => 99934002,
);
$db->delete_row(
    table => "$table",
    col => 'id',
    val => 99934003,
);
