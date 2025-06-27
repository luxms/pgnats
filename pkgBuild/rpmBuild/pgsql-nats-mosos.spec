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
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-server-devel
BuildRequires:  cargo-pgrx openssl clang
Disttag:        mosos
Distribution:   mosos/15.5/x86_64


%description
NATS connect for PostgresPRO


%install
cd %{_topdir}

cargo pgrx init --pg%{pg_ver} /usr/lib/postgresql%{pg_ver}/bin/pg_config --skip-version-check
cargo pgrx package --pg-config /usr/lib/postgresql%{pg_ver}/bin/pg_config
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgsql-%{pg_ver}-nats%{dist}
%{__mkdir_p} %{buildroot}/usr/lib/postgresql%{pg_ver}/lib64 %{buildroot}/usr/share/postgresql%{pg_ver}/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/lib/postgresql%{pg_ver}/lib64/* %{buildroot}/usr/lib/postgresql%{pg_ver}/lib64/
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/share/postgresql%{pg_ver}/extension/* %{buildroot}/usr/share/postgresql%{pg_ver}/extension/
rm -rf target


%files
/usr/lib/postgresql%{pg_ver}/lib64
/usr/share/postgresql%{pg_ver}/extension

%changelog
* Fri Nov 15 2024 Vladislav Semikin <repo@luxms.com>
- Initial Package.
