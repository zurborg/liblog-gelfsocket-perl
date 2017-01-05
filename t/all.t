#!perl

use Test::More;
use IO::Socket::UNIX;
use autodie;
use JSON qw(decode_json);
use Log::GelfSocket;
use Sys::Hostname;
use Time::HiRes qw(time);
use Socket qw(SOCK_DGRAM SOMAXCONN MSG_DONTWAIT);

my $sockfile = './gelf.sock';

if (-e $sockfile) {
    BAIL_OUT("file $sockfile exists");
}

my $listener = IO::Socket::UNIX->new(
    Type => SOCK_DGRAM,
    Local => $sockfile,
    Listen => SOMAXCONN,
) or BAIL_OUT("cannot listen on unix domain socket file $sockfile: $!");

sub readlog {
    $listener->recv(my $json, 2**16, MSG_DONTWAIT);
    return unless defined $json and length $json;
    use bytes;
    my $len = unpack(n => substr($json, 0, 2));
    my $gelf = decode_json(substr($json, 2, $len));
}


my $logger = Log::GelfSocket->new(
    socket => $sockfile,
);

subtest all => sub {
    my $time0 = time;
    $logger->log(info => "hej");
    my $time1 = time;

    my $gelf = readlog;
    isa_ok ($gelf, 'HASH');
    is($gelf->{host}, hostname(), 'hostname');
    is($gelf->{level}, 7, 'level');
    is($gelf->{version}, '1.1', 'version');
    is($gelf->{message}, 'hej', 'message');
    cmp_ok ($gelf->{timestamp}, '>', $time0, 'timestamp (before)');
    cmp_ok ($gelf->{timestamp}, '<', $time1, 'timestamp (after)');
};

subtest additional => sub {
    $logger->log(info => "hej", foo => 123, bar => 456);
    my $gelf = readlog;
    is($gelf->{_foo}, 123, 'additional(foo)');
    is($gelf->{_bar}, 456, 'additional(foo)');
};

subtest order => sub {
    $logger->log(info => "first");
    $logger->log(info => "second");
    $logger->log(info => "third");
    my $gelf;
    $gelf = readlog;
    is($gelf->{message}, 'first', 'first message');
    $gelf = readlog;
    is($gelf->{message}, 'second', 'second message');
    $gelf = readlog;
    is($gelf->{message}, 'third', 'third message');
};

subtest empty => sub {
    $gelf = readlog;
    is($gelf, undef);
};

$logger->close;

$listener->close;

unlink $sockfile;

done_testing;
