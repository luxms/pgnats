%global         __brp_check_rpaths %{nil}
%define         _build_id_links   none

%{!?version:    %define version %{VERSION}}
%{!?pg_ver:     %define pg_ver 13}

%if 0%{?redos}   == 07
%define          dist .redos%{redos_ver}
%endif

Name:           pgsql%{pg_ver}-nats
Summary:        HTTP client for PostgreSQL
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

%if 0%{?el7} && 0%{?redos} == 0
Requires:       postgresql%{pg_ver}-server
BuildRequires:  postgresql%{pg_ver}-devel
Disttag:        el%{rhel}
Distribution:   el/%{rhel}/x86_64
%endif

%if 0%{?el8} || 0%{?el9}
Requires:       postgresql-server >= %{pg_ver} postgresql-server < %(echo $((%{pg_ver} + 1)))
Requires:       policycoreutils-python-utils
BuildRequires:  postgresql-server-devel >= %{pg_ver} postgresql-server-devel < %(echo $((%{pg_ver} + 1)))
BuildRequires:  perl-IPC-Cmd perl-Pod-Html libtool gettext-devel
Disttag:        el%{rhel}
Distribution:   el/%{rhel}/x86_64
%endif


%description
NATS connect for PostgreSQL

%if 0%{?redos}
%package        -n pgpro%{pg_ver}-nats
Summary:        HTTP client for PostgresPro
Requires:       postgrespro-std-%{pg_ver}-server policycoreutils-python-utils
BuildRequires:  postgrespro-std-%{pg_ver}-devel
Provides:       pgpro%{pg_ver}-nats

%description    -n pgpro%{pg_ver}-nats
NATS connect for PostgresPRO

%package        -n pgpro%{pg_ver}ent-nats
Summary:        HTTP client for PostgresPro-ent
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
cargo install cargo-pgrx --version 0.12.9 --locked

%if 0%{?el8} || 0%{?el9}
%{__make} PG_CONFIG=/usr/bin/pg_server_config clean
%make_install PG_CONFIG=/usr/bin/pg_server_config
%{_topdir}/trivy-scan.sh ./ %{name}%{dist}
%else
%{__make} PG_CONFIG=/usr/pgsql-%{pg_ver}/bin/pg_config clean
%make_install PG_CONFIG=/usr/pgsql-%{pg_ver}/bin/pg_config
%{_topdir}/trivy-scan.sh ./ %{name}%{dist}
%endif
%if 0%{?redos}
cargo pgrx init --pg13 "/usr/pgsql-%{pg_ver}/bin/pg_config"
%endif

cargo pgrx package
#%{_topdir}/trivy-scan.sh ./ pgpro%{pg_ver}-http%{dist}


%files
%if 0%{?el8} || 0%{?el9}
/usr/lib64/pgsql
/usr/share/pgsql/extension
%else
/usr/pgsql-%{pg_ver}/lib/
/usr/pgsql-%{pg_ver}/share/extension
%endif
%{_datadir}/pgsql-http/selinux/pgsql-http.pp

%if 0%{?redos}
%files -n pgpro%{pg_ver}-http
/opt/pgpro/std-%{pg_ver}/lib/
/opt/pgpro/std-%{pg_ver}/share/extension
%{_datadir}/pgsql-http/selinux/pgsql-http.pp

%files -n pgpro%{pg_ver}ent-http
/opt/pgpro/ent-%{pg_ver}/lib/
/opt/pgpro/ent-%{pg_ver}/share/extension
%{_datadir}/pgsql-http/selinux/pgsql-http.pp
%endif

%changelog
* Thu Jun 6 2024 Vladislav Semikin <repo@luxms.com>
- change RPM specifications
* Fri Sep 15 2023 Vladislav Semikin <repo@luxms.com>
- add Rocky9 condition
* Thu Sep 14 2023 Vladislav Semikin <repo@luxms.com>
- Edit SELinux policy.
* Thu Aug 04 2022 Dmitriy Kovyarov <dmitrii.koviarov@yasp.ru>
- Added statiс curl lib.
* Thu Jul 21 2022 Dmitriy Kovyarov <dmitrii.koviarov@yasp.ru>
- Initial Package.
