%global         __brp_check_rpaths %{nil}
%define         _build_id_links   none

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
Source0:        https://github.com/luxms/pgnats/archive/refs/heads/main.zip
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-server-devel pkg-config unzip openssl-devel

Disttag:        alt10
Distribution:   alt/p10/x86_64/RPMS.thirdparty/

%description
NATS connect for PostgreSQL


%prep
%{__rm} -rf %{name}-%{version}
unzip %{SOURCE0}
%{__mv} pgnats-main %{name}-%{version}


%install
cd %{name}-%{version}
cargo install cargo-pgrx --version `cat .cargo-pgrx-version` --locked
cargo pgrx init --pg%{pg_ver} "/usr/bin/pg_config"
cargo pgrx package --pg-config /usr/bin/pg_config
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgsql-%{pg_ver}-nats%{dist}
%{__mkdir_p} %{buildroot}/usr/lib64/pgsql %{buildroot}/usr/share/pgsql/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/lib/ %{buildroot}/usr/lib64/pgsql/
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/share/extension/ %{buildroot}/usr/share/pgsql/extension/


%files
/usr/lib64/pgsql/
/usr/share/pgsql/extension/


%changelog
* Thu Mar 06 2025 Dmitriy Kovyarov <dmitrii.koviarov@yasp.ru>
- Initial Package.
