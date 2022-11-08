# Based on https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/9/html/installing_and_using_dynamic_programming_languages/assembly_packaging-python-3-rpms_installing-and-using-dynamic-programming-languages
# merged with python3 setup.py bdist_rpm --spec-only result.

%define srcname tarantool
%define version %(python3 setup.py --version)

Name:           python-%{srcname}
Version:        %{version}
Release:        1%{?dist}
Summary:        Python client library for Tarantool

License:        BSD
Group:          Development/Libraries
URL:            https://github.com/tarantool/tarantool-python

BuildArch:      noarch
Source:         %{srcname}-%{version}.tar.gz
Vendor:         tarantool-python AUTHORS <admin@tarantool.org>

BuildRequires:  python3-setuptools
BuildRequires:  python3-wheel

%global _description %{expand:
Python client library for Tarantool.}

%description %_description


%package -n python3-%{srcname}

Requires:       python3-msgpack
Requires:       python3-pandas
Requires:       python3-pytz

Summary:        %{summary}

Obsoletes:      tarantool-python <= 0.9.0

%description -n python3-%{srcname} %_description


%prep
%setup -n %{srcname}-%{version}


%build
python3 setup.py build


%install
python3 setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES


%clean
rm -rf $RPM_BUILD_ROOT


%files -n python3-%{srcname} -f INSTALLED_FILES


%defattr(-,root,root)
