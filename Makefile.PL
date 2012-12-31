use 5.010000;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'DB::SQL::Migrations',
    VERSION_FROM      => 'lib/DB/SQL/Migrations.pm', # finds $VERSION
    PREREQ_PM         => {
      'Mojo' => 0,
      'DBIx::MultiStatementDo' => 0,
      'File::Basename' => 0,

    }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/DB/SQL/Migrations.pm', # retrieve abstract from module
       AUTHOR         => 'Jonathan Taylor <jon@local>') : ()),
);
