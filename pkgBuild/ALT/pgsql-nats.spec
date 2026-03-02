%global         __brp_check_rpaths %{nil}
%define         _build_id_links   none
%global         __find_debuginfo_files %{nil}
%define         debug_package  %{nil}

%{!?version:    %define version %{VERSION}}
%{!?release:    %define release %(date +%%Y%%m%%d)}
%define         dist alt10

Name:           pgpro15-nats
Summary:        NATS client for PostgresPro
Version:        %{version}
Release:        %{release}.%{?dist}
Vendor:         YASP Ltd, Luxms Group
URL:            https://github.com/luxms/pgnats
License:        CorpGPL
Group:		    Databases
Requires:       postgrespro-std-15-server
BuildRequires:  postgrespro-std-15-devel
BuildRequires:  cargo-pgrx openssl clang
SOURCE0:		pgsql-nats-v%{version}.tar.gz
Disttag:        alt10
%description
NATS connect for PostgresPRO

%package        -n pgpro17-nats
Summary:        NATS client for PostgresPro
Group:		    Databases
Requires:       postgrespro-std-17-server
BuildRequires:  postgrespro-std-17-devel
%description    -n pgpro17-nats
NATS connect for PostgresPRO


%prep
%{__mkdir_p} %{_builddir}/%{name}-%{version}
if [[ ! -f %{_builddir}/%{name}-%{version}/Cargo.toml ]]; then
  %{__tar} -zxf %{SOURCE0} -C %{_builddir}/%{name}-%{version} --strip-components 1
fi

%install
cd %{_builddir}/%{name}-%{version}

cargo pgrx init --pg15 /opt/pgpro/std-15/bin/pg_config --skip-version-check
cargo pgrx package --pg-config /opt/pgpro/std-15/bin/pg_config
%{__mkdir_p} %{buildroot}/opt/pgpro/std-15/lib %{buildroot}/opt/pgpro/std-15/share/extension
%{__mv} target/release/pgnats-pg15/opt/pgpro/std-15/lib/* %{buildroot}/opt/pgpro/std-15/lib/
%{__mv} target/release/pgnats-pg15/opt/pgpro/std-15/share/extension/* %{buildroot}/opt/pgpro/std-15/share/extension/
rm -rf target

cargo pgrx init --pg17 /opt/pgpro/std-17/bin/pg_config --skip-version-check
cargo pgrx package --pg-config /opt/pgpro/std-17/bin/pg_config
%{__mkdir_p} %{buildroot}/opt/pgpro/std-17/lib %{buildroot}/opt/pgpro/std-17/share/extension
%{__mv} target/release/pgnats-pg17/opt/pgpro/std-17/lib/* %{buildroot}/opt/pgpro/std-17/lib/
%{__mv} target/release/pgnats-pg17/opt/pgpro/std-17/share/extension/* %{buildroot}/opt/pgpro/std-17/share/extension/
rm -rf target

%files
/opt/pgpro/std-15/lib/
/opt/pgpro/std-15/share/extension

%files -n pgpro17-nats
/opt/pgpro/std-17/lib/
/opt/pgpro/std-17/share/extension

%changelog
* Thu Mar 06 2025 Dmitriy Kovyarov <dmitrii.koviarov@yasp.ru>
- Initial Package.
