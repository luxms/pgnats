%global         __brp_check_rpaths %{nil}
%define         _build_id_links   none
%define         debug_package  %{nil}

%{!?version:    %define version %{VERSION}}
%{!?pg_ver:     %define pg_ver 15}
%define         dist alt10

Name:           pgsql%{pg_ver}-nats
Summary:        NATS connect for PostgreSQL
Version:        %{version}
Release:        1.%{?dist}
Vendor:         YASP Ltd, Luxms Group
URL:            https://github.com/luxms/pgnats
License:        CorpGPL
Group:		    Databases
Source0:        https://github.com/luxms/pgnats/archive/refs/heads/fix/pgpro.zip
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-server-devel pkg-config unzip openssl-devel

Disttag:        alt10
Distribution:   alt/p10/x86_64/RPMS.thirdparty/

%description
NATS connect for PostgreSQL

%package        -n pgpro%{pg_ver}-nats
Summary:        HTTP client for PostgresPro
Group:		    Databases
Requires:       postgrespro-std-%{pg_ver}-server
BuildRequires:  postgrespro-std-%{pg_ver}-devel
Provides:       pgpro%{pg_ver}-nats

%description    -n pgpro%{pg_ver}-nats
NATS connect for PostgresPRO


%prep
%{__rm} -rf %{name}-%{version}
unzip %{SOURCE0}
%{__mv} pgnats-fix-pgpro %{name}-%{version}


%install
cd %{name}-%{version}
cargo install cargo-pgrx --git https://github.com/luxms/pgrx

cargo pgrx init --pg%{pg_ver} /usr/bin/pg_config --skip-version-check
cargo pgrx package --pg-config /usr/bin/pg_config
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgsql-%{pg_ver}-nats%{dist}
%{__mkdir_p} %{buildroot}/usr/lib64/pgsql %{buildroot}/usr/share/pgsql/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/lib/ %{buildroot}/usr/lib64/pgsql/
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/share/extension/ %{buildroot}/usr/share/pgsql/extension/
rm -rf target

cargo pgrx init --pg%{pg_ver} "/opt/pgpro/std-%{pg_ver}/bin/pg_config"
cargo pgrx package --pg-config /opt/pgpro/std-%{pg_ver}/bin/pg_config
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgpro%{pg_ver}-nats%{dist}
%{__mkdir_p} %{buildroot}/opt/pgpro/std-%{pg_ver}/lib %{buildroot}/opt/pgpro/std-%{pg_ver}/share/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/std-%{pg_ver}/lib/ %{buildroot}/opt/pgpro/std-%{pg_ver}/lib/
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/std-%{pg_ver}/share/extension/ %{buildroot}/opt/pgpro/std-%{pg_ver}/share/extension/
rm -rf target


%files
/usr/lib64/pgsql/
/usr/share/pgsql/extension/

%files -n pgpro%{pg_ver}-nats
/opt/pgpro/std-%{pg_ver}/lib/
/opt/pgpro/std-%{pg_ver}/share/extension

%changelog
* Thu Mar 06 2025 Dmitriy Kovyarov <dmitrii.koviarov@yasp.ru>
- Initial Package.
