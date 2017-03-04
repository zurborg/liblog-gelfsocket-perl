use strictures 2;

package Log::GelfSocket;

# ABSTRACT: Log GELF messages via unix domain socket

use IO::Socket;
use Socket qw(SOCK_DGRAM);
use JSON 2.90;
use Time::HiRes 1.9726;
use Sys::Hostname;
use Time::Monotonic;
use Encode qw(decode_utf8);
use Carp qw(croak carp);
use Scalar::Util qw(blessed);
use Moo 2;
use boolean;

use constant GELF_SPEC_VERSION  => '1.1';
use constant HOSTNAME           => hostname();

BEGIN { $Carp::Internal{+__PACKAGE__}++ }

sub carp_or_croak
{
    boolean(shift) ? croak(@_) : carp(@_);
    return false;
}

=head1 DESCRIPTION

Send log messages to a local unix domain socket.

Before using this module you need an active L<gelf-broker> instance listening on the unix domain socket specified in the L</socket> attribute. L<gelf-broker> is shipped with this distribution.

=head1 SYNOPSIS

    $logger = Log::GelfSocket->new;
    $logger->log(info => "This is a test message.");

=cut

=attr socket

The unix domain datagram socket to connect to.

This is a read-only value and can only be set in the constructor.

Defaults to I</var/run/gelf.sock>.

=cut

has socket      => (
    is          => 'ro',
    default     => '/var/run/gelf.sock',
);

=attr ticker

Initializes a ticker by passing the amount of seconds after that messages should be flushed via L</tick>.

=cut

has ticker      => (
    is          => 'rw',
    predicate   => true,
    clearer     => true,
);

=attr timer

A L<Time::Monotonic> timer for L</ticker>. Should only be used read-only.

=cut

has timer       => (
    is          => 'lazy',
    clearer     => true,
    default     => sub { Time::Monotonic->new },
);

=attr autoflush

Flush message to socket after each call to L</log>.

Enabled by default.

=cut

has autoflush   => (
    is          => 'rw',
    default     => sub { true },
    coerce      => \&boolean,
);

=attr autotick

When autoflush is disabled, call L</tick> after each call to L</log>.

Disabled by default.

=cut

has autotick    => (
    is          => 'rw',
    default     => sub { false },
    coerce      => \&boolean,
);

has _handle     => (
    is          => 'rw',
    clearer     => true,
);

has _json       => (
    is          => 'ro',
    default     => sub { JSON->new->utf8->allow_nonref },
);

has _buffer     => (
    is          => 'ro',
    default     => sub { [] },
);

=attr default_level

When I<level> is undefined, use this default level

=cut

has default_level   => (
    is              => 'rw',
);

sub BUILDARGS
{
    my $class = shift;
    return { @_ }
}

=head1 LOG LEVELS

These levels are known and supposed to be compatible to various other logging engines.

    Identifier | Numeric level
    -----------+--------------
    fatal      | 1
    emerg      | 1
    emergency  | 1
    -----------+---
    alert      | 2
    -----------+---
    crit       | 3
    critical   | 3
    -----------+---
    error      | 4
    err        | 4
    -----------+---
    warn       | 5
    warning    | 5
    -----------+---
    note       | 6
    notice     | 6
    -----------+---
    info       | 7
    -----------+---
    debug      | 8
    -----------+---
    trace      | 9
    core       | 9

=cut

use constant LEVELS => {

    ###############
    fatal     => 1,
    emerg     => 1,
    emergency => 1,
    ###############
    alert     => 2,
    ###############
    crit      => 2,
    critical  => 3,
    ###############
    error     => 4,
    err       => 4,
    ###############
    warn      => 5,
    warning   => 5,
    ###############
    note      => 6,
    notice    => 6,
    ###############
    info      => 7,
    ###############
    debug     => 8,
    ###############
    trace     => 9,
    core      => 9,
    ###############

};

sub _trim
{
    shift
    =~ s{\r+}{}sgr
    =~ s{^\s+}{}sr
    =~ s{\s+$}{}sr
}

=method log

    $logger->log($level, $message, %additional_gelf_parameters);
    $logger->log(alert => "This is an alert!");
    $logger->log(notice => "Look at this.", additional_param => $additional_value);

Additional GELF params must be prefixed with an underscore - but this method does that for you.

Overrides are only allowed for I<host>/I<hostname> and I<timestamp>/I<time> params. They defaults to the system hostname and the current timestamp from L<Time::HiRes/time>.

Croaks if there is an error.

=cut

sub _prepare
{
    my $self = shift;
    my ($level, $message, %gelf) = @_;

    $level ||= $self->default_level;

    croak "log message without level"   unless defined $level;
    croak "log message without message" unless defined $message;

    # replace level with numeric code, if needed
    $level = LEVELS->{lc($level)} unless $level =~ m{^[1-9]$};

    # additional fields are only allowed with a prefixed underscore
    # and strip off all unallowed chars
    %gelf = map {
        m{^_[\w\.\-]+$}i
    ?
        (
            lc($_)
        ,
            $gelf{$_}
        )
    :
        (
            '_'.s{[^\w\.\-]+}{}gr
        ,
            $gelf{$_}
        )
    } grep { defined $gelf{$_} } keys %gelf;

    # graylog omit the id field automatically
    if (exists $gelf{_id}) {
        carp "log message with id is not allowed";
        delete $gelf{_id};
    }

    # preserve params, which are allowed by client
    # including some mispelled ones
    $gelf{host}             = delete $gelf{_hostname}       if defined $gelf{_hostname};
    $gelf{host}             = delete $gelf{_host}           if defined $gelf{_host};
    $gelf{timestamp}        = delete $gelf{_time}           if defined $gelf{_time};
    $gelf{timestamp}        = delete $gelf{_timestamp}      if defined $gelf{_timestamp};
    $message        .= "\n" . delete $gelf{_message}        if defined $gelf{_message};
    $message        .= "\n" . delete $gelf{_short_message}  if defined $gelf{_short_message};
    $message        .= "\n" . delete $gelf{_full_message}   if defined $gelf{_full_message};

    # hostname defaults to system hostname...
    $gelf{host} ||= HOSTNAME;

    # ...and timestamp with milliseconds by default
    $gelf{timestamp} ||= Time::HiRes::time();

    if (blessed $gelf{timestamp} and $gelf{timestamp}->isa('DateTime')) {
        $gelf{timestamp} = $gelf{timestamp}->hires_epoch;
    }

    # graylog seems to have problems with float values in json
    # so force string, which works fine
    $gelf{timestamp} = ''.$gelf{timestamp};

    $message = _trim($message);

    if ($message =~ m{\n}s) {
        my ($short, $full) = split m{\n}s, $message, 2;
        $gelf{short_message} = _trim(join("\n", map { $_ // '' } ($short, $gelf{short_message})));
        $gelf{ full_message} = _trim(join("\n", map { $_ // '' } ($full , $gelf{ full_message})));
    } else {
        $gelf{message} = $message;
    }

    $gelf{version}  = GELF_SPEC_VERSION;
    $gelf{level}    = $level;

    if (wantarray) {
        return %gelf;
    } else {
        return decode_utf8($self->_json->encode(\%gelf));
    }
}

sub log
{
    my $self = shift;

    my $json = $self->_prepare(@_);

    push @{$self->_buffer} => $json;

    my $strict;

    if ($self->autoflush) {
        $self->flush($strict);
    } elsif ($self->autotick) {
        $self->tick($strict);
    }

    return $self;
}

=method flush

Flushes all outstanding messages at once. Returns true on success.

If there is an error, a warning is emitted by I<carp> and false is returned.

=cut

sub flush
{
    my $self = shift;

    my $strict = boolean($_[0]);

    unless ($self->_handle) {
        my $file = $self->socket;
        my $socket = IO::Socket::UNIX->new(
            Type => SOCK_DGRAM,
            Peer => $file,
        );
        my $error = $!;
        if ($socket) {
            $self->_handle($socket);
        } elsif ($error) {
            undef $socket;
            return carp_or_croak($strict, "cannot connect to socket $file: $error");
        } else {
            die "no socket and no error";
        }
    }

    use bytes;

    while (@{$self->_buffer}) {
        my $message = shift(@{$self->_buffer}) || next;
        my $length = length $message;
        my $sent = $self->_handle->send($message);
        my $error = $!;
        if (defined $sent) {
            if ($sent == $length) {
                next;
            } elsif ($error) {
                push @{$self->_buffer} => $message;
                $self->close;
                return carp_or_croak("only sent $sent bytes: $error");
            } else {
                die "only sent $sent bytes but got no error";
            }
        } elsif ($error) {
            push @{$self->_buffer} => $message;
            $self->close;
            return carp_or_croak("send error: $error");
        }
    }

    return true;
}

=method close

Closes the current handle.

=cut

sub close
{
    my $self = shift;
    if ($self->_handle) {
        $self->_handle->close;
        $self->_clear_handle;
    }
    return $self;
}

=method tick

Checks whether the timer is greater than the L</ticker> value and call L</flush>.

=cut

sub tick
{
    my $self = shift;
    my $strict = boolean(shift);
    return 'no ticker' unless $self->has_ticker;
    return 'not ready' unless $self->timer->now > $self->ticker;
    return 'not empty' unless $self->flush($strict);
    return 'not clear' unless $self->clear_timer;
    return 'no ticker' unless $self->timer->now;
    return;
}

=head1 TICKER

When you produce hundreds and thousand messages at once, you can throttle the message throughput by flushing messages occasionally.

There are two independent ways to do it:

=over 4

=item * Defer call to L</flush>

Disable autoflush and call L</flush> occasionally by your own.

    $logger = Log::GelfSocket->new(
        autoflush => false,
    );
    # ... or ...
    $logger->autoflush(false);

    $logger->log(...); # does not flush any messages

    $logger->flush; # flushes all outstanding messages

=item * Initialize timer

Disable autoflush and define a timeout value

    $logger = Log::GelfSocket->new(
        autoflush => false,
        ticker => 5, # seconds
    );
    # ... or ...
    $logger->autoflush(false);
    $logger->timer(5);

    $logger->tick; # timer started, not flushing
    # ... 2 seconds later ...
    $logger->tick; # timeout not reached yet
    # ... 3 seconds or more later ...
    $logger->tick; # timeout reached, flush messages, reset timer
    # ... and so on ...

=item * Initialize timer with autoticking feature

Disable autoflush, define a timeout value and enable autotick

    $logger = Log::GelfSocket->new(
        autoflush => false,
        ticker => 5,
        autotick => true,
    );
    # ... or ...
    $logger->autoflush(false);
    $logger->ticker(5);
    $logger->autotick(true);

    $logger->log(...); # first call starts timer
    # message(s) not flushed yet
    # ... min. 5 seconds later ...
    $logger->log(...); # on next call, timer is greater than 5 seconds
    # now all messages are flushed

=back

=cut

=for Pod::Coverage BUILDARGS has_ticker carp_or_croak

=cut

1;
