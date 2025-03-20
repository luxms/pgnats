%global         __brp_check_rpaths %{nil}
%define         _build_id_links   none
%define         debug_package  %{nil}

%{!?version:    %define version %{VERSION}}
%{!?pg_ver:     %define pg_ver 15}

%define         dist .mosos


Name:           pgsql%{pg_ver}-nats
Summary:        NATS connect for PostgreSQL
Version:        %{version}
Release:        1%{?dist}
Vendor:         YASP Ltd, Luxms Group
URL:            https://github.com/pramsey/pgsql-nats
License:        CorpGPL
Source0:        https://github.com/luxms/pgnats/archive/refs/heads/main.zip
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-server-devel pkg-config unzip openssl-devel clang
Disttag:        mosos
Distribution:   mosos/15.5/x86_64


%description
NATS connect for PostgresPRO


%prep
%{__rm} -rf %{name}-%{version}
unzip %{SOURCE0}
%{__mv} pgnats-main %{name}-%{version}


%install
cd %{name}-%{version}
cargo install cargo-pgrx --git https://github.com/luxms/pgrx

cargo pgrx init --pg%{pg_ver} /usr/lib/postgresql%{pg_ver}/bin/pg_config --skip-version-check
cargo pgrx package --pg-config /usr/lib/postgresql%{pg_ver}/bin/pg_config
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgsql-%{pg_ver}-nats%{dist}
%{__mkdir_p} %{buildroot}/usr/lib/postgresql%{pg_ver}/lib64 %{buildroot}/usr/share/postgresql%{pg_ver}/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/lib/ %{buildroot}/usr/lib/postgresql%{pg_ver}/lib64/
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/share/extension/ %{buildroot}/usr/share/postgresql%{pg_ver}/extension/
rm -rf target


%files
/usr/lib/postgresql%{pg_ver}/lib64
/usr/share/postgresql%{pg_ver}/extension

%changelog
* Fri Nov 15 2024 Vladislav Semikin <repo@luxms.com>
- Initial Package.
