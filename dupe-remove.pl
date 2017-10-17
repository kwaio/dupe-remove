#!/usr/bin/perl
use strict;
use warnings;
use Digest::SHA;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
my $DEBUG = 0;

my $backup_path = "/backups/*/[0-1]/";		## Use this for find shell call
my @args = ();
#my @args = qw(-size +64k);			## Extra find arguments - ie only return files over 64Kb.
my $quick_hash_kb = "128";			## Size chunk in kb to use as a 'quick hash'.
my $logfile = "/root/dupe-remove.log";

## Declare variables...
my %files;
my $logical_file_size = 0;
my $physical_file_size = 0;
my $num_files = 0;
$| = 1;

open (LOGFILE, '>' . $logfile);

sub logger ($) {
	my $msg = shift();
	print "\n" . localtime() . ": " . $msg;
	print LOGFILE "\n" . localtime() . ": " . $msg;
}

logger("Deduplication Process started");
logger("Building list of files");
my @backup_paths = glob $backup_path or die "$0: No paths found for $backup_path\n";
open my $find, '-|', 'find', @backup_paths, @args, qw(-type f -printf %p\0%s\0%i\0%n\n) or die "$0: find: $!\n";
while (<$find>) {
	chomp();
	$num_files++;
	if ( $num_files % 1000 == 0 ) { print "."; }
	my ($filename, $size, $inode, $nlinks) = split(/\0/, $_);
	if ( !exists $files{$inode} ) { 
		$physical_file_size = $physical_file_size + $size;
	}
	$logical_file_size = $logical_file_size + $size;
	$files{$inode}{size} = $size;
	$files{$inode}{inode} = $inode;
	$files{$inode}{nlinks} = $nlinks;
	push( @{ $files{$inode}{filename} }, $filename );
}
close $find;
logger("File list complete!");

if ( $DEBUG != 0 ) {
	open (DUMPFILE, '>' . "/root/files.txt");
	foreach my $inode ( sort {$a <=> $b} keys %files) {
		print DUMPFILE Dumper($files{$inode});
	}
	close DUMPFILE;
}

logger("Filtering file list... ");
## Create a list of files sorted by file size as the primary filter.
## This allows us to ONLY assess files for hashing that are of the same filesize
## as logically, files of difference sizes cannot be identical.
my %filesizes;
foreach my $inode ( sort {$a <=> $b} keys %files) {
	push @{ $filesizes{$files{$inode}{size}} }, $files{$inode};
}
foreach (keys %filesizes) {
	if ( @{$filesizes{$_}} == 1 ) {
		delete $filesizes{$_};
	}
}
logger("Done!");

if ( $DEBUG != 0 ) {
	open (DUMPFILE, '>' . "/root/filesizes.txt");
	print DUMPFILE Dumper(%filesizes);
	close DUMPFILE;
}

logger("Performing quick hash...");
my $num_quick_hashes = 0;
my %quickhashlist;
my %hashlist;
foreach my $size ( keys %filesizes ) {
	foreach my $file ( @{$filesizes{$size}} ) {
		$num_quick_hashes++;
		if ( $num_quick_hashes % 500 == 0 ) { print "."; }
		open my $fh, '<', $file->{filename}[0] or die "Unable to open file: $!\n";
			binmode $fh;
			my $quick_hash_data;
			my $bytes_read = sysread $fh, $quick_hash_data, $quick_hash_kb * 1024;
		close $fh;
		my $quickhash = Digest::SHA->new->add($quick_hash_data)->hexdigest;
		$files{$file->{inode}}{quickhash} = $quickhash;
		
		## Shortcut if we've hashed the whole file, put it straight in the final
		## hashlist...
		if ( $bytes_read < $quick_hash_kb * 1024 ) {
			push @{$hashlist{$quickhash}}, $files{$file->{inode}};
			$files{$file->{inode}}{quickhash_shortcut} = "true";
		} else {
			push @{$quickhashlist{$quickhash}}, $files{$file->{inode}};
		}
	}
}
logger("Done!");

logger("Filtering quick hashes...");
foreach (keys %quickhashlist) {
	if ( @{$quickhashlist{$_}} == 1 ) {
		delete $quickhashlist{$_};
	}
}
logger("Filter complete!");

logger("Calculating Full Hashes...");
my $num_hashes = 0;
foreach my $quickhash ( keys %quickhashlist ) {
	foreach my $file ( @{$quickhashlist{$quickhash}} ) {
		$num_hashes++;
		if ( $num_hashes % 500 == 0 ) { print "."; }
		my $hash = Digest::SHA->new->addfile($file->{filename}[0])->hexdigest;
		$files{$file->{inode}}{hash} = $hash;
		push @{$hashlist{$hash}}, $files{$file->{inode}};
	}
}
logger("Full Hashes Complete!");

logger("Sorting Hashes...");
foreach (keys %hashlist) {
	if ( @{$hashlist{$_}} == 1 ) {
		delete $hashlist{$_};
	}
}
logger("Hash Sort Complete!");

if ( $DEBUG != 0 ) {
	open (DUMPFILE, '>' . "/root/hashlist.txt");
	print DUMPFILE Dumper(%hashlist);
	close DUMPFILE;
}

logger("Creating hard links...\n");
my $num_hardlinks = 0;
my $bytes_saved = 0;
my $logbuffer;
foreach my $hash ( keys %hashlist ) {
	my $linksource;
	## Find the entry with the most links and use it as the source.
	foreach my $file ( @{$hashlist{$hash}} ) {
		if ( !exists($linksource->{filename}[0]) or $linksource->{nlinks} < $file->{nlinks} ) {
			$linksource = $file;
		}
	}

	## Go through each file and link it to the linksource.
	foreach my $file ( @{$hashlist{$hash}} ) {
		next if $file->{inode} eq $linksource->{inode};

		## Loop through each filename in the array
		foreach my $duplicate ( @{$file->{filename}} ) {
			$logbuffer .= "Linking " . $linksource->{filename}[0] . " (i:" . $linksource->{inode} . ", n:" . $linksource->{nlinks} . ")\n => " . $duplicate . " (i:" . $file->{inode} . ", n: " . $file->{nlinks} . ")\n\n";
			link $linksource->{filename}[0], $duplicate . $$ or die "Unable to link file " . $linksource->{filename}[0] . " to " . $duplicate . $$ . " - $!";
			rename $duplicate . $$, $duplicate or die "Unable to rename " . $duplicate . $$ . " to " . $duplicate . " - $!";
			$bytes_saved = $bytes_saved + $file->{size};
			$linksource->{nlinks}++;
			if ( $num_hardlinks % 1000 == 0 ) {
				print LOGFILE $logbuffer;
				$logbuffer = "";
				print ".";
			}
			$num_hardlinks++;
		}
	}

}

if ( $logbuffer ) {
	print LOGFILE $logbuffer;
}
logger("Hard links Complete!");

## Fix some stats...
1 while $bytes_saved =~ s/^(-?\d+)(\d\d\d)/$1,$2/;
1 while $logical_file_size =~ s/^(-?\d+)(\d\d\d)/$1,$2/;
1 while $physical_file_size =~ s/^(-?\d+)(\d\d\d)/$1,$2/;

logger("\n\n\tDEDUPE SUMMARY:");
logger("Number of files checked: " . $num_files);
logger("Number of files Quick Hashed: " . $num_quick_hashes);
logger("Number of files Full Hashed: " . $num_hashes);
logger("Hardlinks Created: " . $num_hardlinks);
logger("Total Logical bytes: " . $logical_file_size);
logger("Total Physical bytes: " . $physical_file_size);
logger("Bytes Saved: " . $bytes_saved . "\n\n");

close (LOGFILE);
