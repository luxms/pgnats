%global         __brp_check_rpaths %{nil}
%define         _build_id_links   none

%{!?version:    %define version %{VERSION}}
%{!?pg_ver:     %define pg_ver 13}

%if 0%{?redos}   == 07
%define          dist .redos%{redos_ver}
%endif

Name:           pgsql%{pg_ver}-nats
Summary:        NATS connect for PostgreSQL
Version:        %{version}
Release:        1%{?dist}
Vendor:         YASP Ltd, Luxms Group
URL:            https://github.com/luxms/pgnats
License:        CorpGPL

Source0:        https://github.com/luxms/pgnats/archive/refs/heads/main.zip

%if 0%{?redos}
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-devel pkg-config unzip openssl-devel
Disttag:        redos%{redos_ver}
Distribution:   redos/%{redos_ver}/x86_64
%endif

%if 0%{?el8} || 0%{?el9}
Requires:       postgresql-server >= %{pg_ver} postgresql-server < %(echo $((%{pg_ver} + 1)))
BuildRequires:  postgresql-server-devel >= %{pg_ver} postgresql-server-devel < %(echo $((%{pg_ver} + 1)))
BuildRequires:  pkg-config unzip openssl-devel
Disttag:        el%{rhel}
Distribution:   el/%{rhel}/x86_64
%endif

%description
NATS connect for PostgreSQL

%if 0%{?redos}
%package        -n pgpro%{pg_ver}-nats
Summary:        NATS connect for PostgresPro
Requires:       postgrespro-std-%{pg_ver}-server policycoreutils-python-utils
BuildRequires:  postgrespro-std-%{pg_ver}-devel
Provides:       pgpro%{pg_ver}-nats

%description    -n pgpro%{pg_ver}-nats
NATS connect for PostgresPRO

%package        -n pgpro%{pg_ver}ent-nats
Summary:        NATS connect for PostgresPro-ent
Requires:       postgrespro-ent-%{pg_ver}-server policycoreutils-python-utils
BuildRequires:  postgrespro-ent-%{pg_ver}-devel
Provides:       pgpro%{pg_ver}ent-nats

%description    -n pgpro%{pg_ver}ent-nats
NATS connect for PostgresPRO-ent
%endif


%prep
%{__rm} -rf %{name}-%{version}
unzip %{SOURCE0}
%{__mv} pgnats-main %{name}-%{version}


%install
cd %{name}-%{version}
cargo install cargo-pgrx --version `cat .cargo-pgrx-version` --locked

%if 0%{?el8} || 0%{?el9}
cargo pgrx init --pg%{pg_ver} "/usr/bin/pg_server_config"
cargo pgrx package
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgsql-%{pg_ver}-nats%{dist}
%{__mv} target/release/pgnats-pg%{pg_ver}/* %{buildroot}/
%endif

%if 0%{?redos}
cargo pgrx init --pg%{pg_ver} "/usr/pgsql-%{pg_ver}/bin/pg_config"
PATH=/usr/pgsql-%{pg_ver}/bin:$PATH cargo pgrx package
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgsql-%{pg_ver}-nats%{dist}
%{__mv} target/release/pgnats-pg%{pg_ver}/* %{buildroot}/
rm -rf target

cargo pgrx init --pg%{pg_ver} "/opt/pgpro/std-%{pg_ver}/bin/pg_config"
PATH=/opt/pgpro/std-%{pg_ver}/bin:$PATH cargo pgrx package
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgpro%{pg_ver}-nats%{dist}
%{__mv} target/release/pgnats-pg%{pg_ver}/* %{buildroot}/
rm -rf target

cargo pgrx init --pg%{pg_ver} "/opt/pgpro/ent-%{pg_ver}/bin/pg_config"
PATH=/opt/pgpro/ent-%{pg_ver}/bin:$PATH cargo pgrx package
%{_topdir}/trivy-scan.sh target/release/pgnats-pg%{pg_ver}/ pgpro%{pg_ver}ent-nats%{dist}
%{__mv} target/release/pgnats-pg%{pg_ver}/* %{buildroot}/
rm -rf target
%endif


%files
%if 0%{?el8} || 0%{?el9}
/usr/lib64/pgsql
/usr/share/pgsql/extension
%endif

%if 0%{?redos}
/usr/pgsql-%{pg_ver}/lib/
/usr/pgsql-%{pg_ver}/share/extension

%files -n pgpro%{pg_ver}-nats
/opt/pgpro/std-%{pg_ver}/lib/
/opt/pgpro/std-%{pg_ver}/share/extension

%files -n pgpro%{pg_ver}ent-nats
/opt/pgpro/ent-%{pg_ver}/lib/
/opt/pgpro/ent-%{pg_ver}/share/extension
%endif

%changelog
* Thu Mar 06 2025 Dmitriy Kovyarov <dmitrii.koviarov@yasp.ru>
- Initial Package.
