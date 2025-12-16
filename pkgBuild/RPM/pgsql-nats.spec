%global         __brp_check_rpaths %{nil}
%define         _build_id_links    none
%define         debug_package      %{nil}

%{!?version:    %define version %{VERSION}}
%{!?release:    %define release 1}
%{!?pg_ver:     %define pg_ver 13}

%if 0%{?redos}   == 07
%define          dist .redos%{redos_ver}
%endif

Name:           pgsql%{pg_ver}-nats
Summary:        NATS connect for PostgreSQL
Version:        %{version}
Release:        %{release}%{?dist}
Vendor:         YASP Ltd, Luxms Group
URL:            https://github.com/luxms/pgnats
License:        CorpGPL
SOURCE0:		%{name}-v%{version}.tar.gz

BuildRequires:  rust rustfmt cargo cargo-pgrx openssl

%if 0%{?redos}
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-devel
Disttag:        redos%{redos_ver}
Distribution:   redos/%{redos_ver}/x86_64
%endif

%if 0%{?el8} || 0%{?el9}

%if 0%{?pg_ver} < 17
Requires:       postgresql-server >= %{pg_ver} postgresql-server < %(echo $((%{pg_ver} + 1)))
BuildRequires:  postgresql-server-devel >= %{pg_ver} postgresql-server-devel < %(echo $((%{pg_ver} + 1)))
%else
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-devel
%endif

BuildRequires:  clang
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
%{__mkdir_p} %{_builddir}/%{name}-%{version}
if [[ ! -f %{_builddir}/%{name}-%{version}/Cargo.toml ]]; then
  %{__tar} -zxf %{SOURCE0} -C %{_builddir}/%{name}-%{version} --strip-components 1
fi

%install

cargo --version
rustc --version

cd %{_builddir}/%{name}-%{version}

%if 0%{?el8} || 0%{?el9}

%if 0%{?pg_ver} < 17
cargo pgrx init --pg%{pg_ver} /usr/bin/pg_server_config
cargo pgrx package --pg-config /usr/bin/pg_server_config
%else
cargo pgrx init --pg%{pg_ver} /usr/pgsql-%{pg_ver}/bin/pg_config
cargo pgrx package --pg-config /usr/pgsql-%{pg_ver}/bin/pg_config
%endif

%{__mv} target/release/pgnats-pg%{pg_ver}/* %{buildroot}/
%endif

%if 0%{?redos}
cargo pgrx init --pg%{pg_ver} /usr/pgsql-%{pg_ver}/bin/pg_config
cargo pgrx package --pg-config /usr/pgsql-%{pg_ver}/bin/pg_config
%{__mkdir_p} %{buildroot}/usr/pgsql-%{pg_ver}/lib %{buildroot}/usr/pgsql-%{pg_ver}/share/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/lib/* %{buildroot}/usr/pgsql-%{pg_ver}/lib/
%{__mv} target/release/pgnats-pg%{pg_ver}/usr/pgsql-%{pg_ver}/share/extension/* %{buildroot}/usr/pgsql-%{pg_ver}/share/extension/
rm -rf target

cargo pgrx init --pg%{pg_ver} /opt/pgpro/std-%{pg_ver}/bin/pg_config
cargo pgrx package --pg-config /opt/pgpro/std-%{pg_ver}/bin/pg_config
%{__mkdir_p} %{buildroot}/opt/pgpro/std-%{pg_ver}/lib %{buildroot}/opt/pgpro/std-%{pg_ver}/share/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/std-%{pg_ver}/lib/* %{buildroot}/opt/pgpro/std-%{pg_ver}/lib/
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/std-%{pg_ver}/share/extension/* %{buildroot}/opt/pgpro/std-%{pg_ver}/share/extension/
rm -rf target

cargo pgrx init --pg%{pg_ver} /opt/pgpro/ent-%{pg_ver}/bin/pg_config
cargo pgrx package --features xid8 --pg-config /opt/pgpro/ent-%{pg_ver}/bin/pg_config
%{__mkdir_p} %{buildroot}/opt/pgpro/ent-%{pg_ver}/lib %{buildroot}/opt/pgpro/ent-%{pg_ver}/share/extension
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/ent-%{pg_ver}/lib/* %{buildroot}/opt/pgpro/ent-%{pg_ver}/lib/
%{__mv} target/release/pgnats-pg%{pg_ver}/opt/pgpro/ent-%{pg_ver}/share/extension/* %{buildroot}/opt/pgpro/ent-%{pg_ver}/share/extension/
rm -rf target
%endif


%files
%if 0%{?el8} || 0%{?el9}

%if 0%{?pg_ver} < 17
/usr/lib64/pgsql
/usr/share/pgsql/extension
%else
/usr/pgsql-%{pg_ver}/lib
/usr/pgsql-%{pg_ver}/share/extension
%endif

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
