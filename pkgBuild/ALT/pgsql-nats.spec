%global         __brp_check_rpaths %{nil}
%define         _build_id_links   none
%global         __find_debuginfo_files %{nil}
%define         debug_package  %{nil}

%{!?version:    %define version %{VERSION}}
%{!?release:    %define release 1}
%{!?pg_ver:     %define pg_ver 15}
%define         dist alt10

Name:           pgpro%{pg_ver}-nats
Summary:        NATS client for PostgresPro
Version:        %{version}
Release:        %{release}.%{?dist}
Vendor:         YASP Ltd, Luxms Group
URL:            https://github.com/luxms/pgnats
License:        CorpGPL
Group:		    Databases
Requires:       postgrespro-std-%{pg_ver}-server
BuildRequires:  postgrespro-std-%{pg_ver}-devel
BuildRequires:  cargo-pgrx openssl clang

Disttag:        alt10
Distribution:   alt/p10/x86_64/RPMS.thirdparty/

%description
NATS connect for PostgresPRO


%install
cd %{_topdir}

cargo pgrx init --pg%{pg_ver} /opt/pgpro/std-%{pg_ver}/bin/pg_config --skip-version-check
cargo pgrx package --pg-config /opt/pgpro/std-%{pg_ver}/bin/pg_config
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgpro%{pg_ver}-nats%{dist}
%{__mkdir_p} %{buildroot}/opt/pgpro/std-%{pg_ver}/lib %{buildroot}/opt/pgpro/std-%{pg_ver}/share/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/std-%{pg_ver}/lib/* %{buildroot}/opt/pgpro/std-%{pg_ver}/lib/
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/std-%{pg_ver}/share/extension/* %{buildroot}/opt/pgpro/std-%{pg_ver}/share/extension/
rm -rf target


%files
/opt/pgpro/std-%{pg_ver}/lib/
/opt/pgpro/std-%{pg_ver}/share/extension

%changelog
* Thu Mar 06 2025 Dmitriy Kovyarov <dmitrii.koviarov@yasp.ru>
- Initial Package.
