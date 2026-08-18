import subprocess
import ldap_lookups


ldap = {"ldap_server":      "MOCKED",
        "ldap_dn":          "MOCKED",
        "ldap_base_dn":     "MOCKED",
        "ldap_password":    "MOCKED",
        "ldap_uid":         "MOCKED",
        "ldap_mail":        "mail",
        "ldap_displayname": "displayname"}


def test_ldap_name(mocker):
    ldapsearch = "first\ndisplayname: Alan Turing\nlast\n"
    cp = subprocess.CompletedProcess(args="",
                                     returncode=0,
                                     stdout=ldapsearch,
                                     stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    expected = "Hi Alan (aturing),"
    actual = ldap_lookups.ldap_lookup_name(user="aturing", ldap=ldap)
    assert actual == expected


def test_ldap_name_missing_displayname(mocker):
    ldapsearch = "first\nname: Alan Turing\nlast\n"
    cp = subprocess.CompletedProcess(args="",
                                     returncode=0,
                                     stdout=ldapsearch,
                                     stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    expected = "Hello aturing,"
    actual = ldap_lookups.ldap_lookup_name(user="aturing", ldap=ldap)
    assert actual == expected


def test_ldap_mail_base64(mocker):
    ldapsearch = "first\ndisplayname:: QWxhbiBUdXJpbmc=\nlast\n"
    cp = subprocess.CompletedProcess(args="",
                                     returncode=0,
                                     stdout=ldapsearch,
                                     stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    expected = "Hi Alan (aturing),"
    actual = ldap_lookups.ldap_lookup_name(user="aturing", ldap=ldap)
    assert actual == expected


def test_ldap_name_using_fullname(mocker):
    ldapsearch = "first\nfullname: Alan Turing\nlast\n"
    cp = subprocess.CompletedProcess(args="",
                                     returncode=0,
                                     stdout=ldapsearch,
                                     stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    expected = "Hi Alan (aturing),"
    ldap_mod = ldap.copy()
    ldap_mod["ldap_displayname"] = "fullname"
    actual = ldap_lookups.ldap_lookup_name(user="aturing", ldap=ldap_mod)
    assert actual == expected


def test_ldap_mail(mocker):
    ldapsearch = "first\nmail: aturing@princeton.edu\nlast\n"
    cp = subprocess.CompletedProcess(args="",
                                     returncode=0,
                                     stdout=ldapsearch,
                                     stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    expected = "aturing@princeton.edu"
    actual = ldap_lookups.ldap_lookup_mail(user="aturing", ldap=ldap)
    assert actual == expected


def test_ldap_mail_missing_mail(mocker):
    ldapsearch = "first\ncontact: aturing@princeton.edu\nlast\n"
    cp = subprocess.CompletedProcess(args="",
                                     returncode=0,
                                     stdout=ldapsearch,
                                     stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    expected = ""
    actual = ldap_lookups.ldap_lookup_mail(user="aturing", ldap=ldap)
    assert actual == expected


def test_ldap_mail_using_contact(mocker):
    ldapsearch = "first\ncontact: aturing@princeton.edu\nlast\n"
    cp = subprocess.CompletedProcess(args="",
                                     returncode=0,
                                     stdout=ldapsearch,
                                     stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    expected = "aturing@princeton.edu"
    ldap_mod = ldap.copy()
    ldap_mod["ldap_mail"] = "contact"
    actual = ldap_lookups.ldap_lookup_mail(user="aturing", ldap=ldap_mod)
    assert actual == expected


# --- base64-encoded mail values ---------------------------------------------
# LDIF uses a double colon for a base64 value. ldapsearch encodes anything it
# cannot write literally, including a value padded with whitespace, so these
# are ordinary addresses that merely arrive encoded. Note the pre-existing
# test_ldap_mail_base64 above covers displayname, not mail.


def _run(mocker, stdout):
    cp = subprocess.CompletedProcess(args="", returncode=0,
                                     stdout=stdout, stderr="")
    mocker.patch("subprocess.run", return_value=cp)
    return ldap_lookups.ldap_lookup_mail(user="aturing", ldap=ldap)


def test_mail_base64_is_decoded(mocker):
    # "alan.turing@institution.edu"
    encoded = "YWxhbi50dXJpbmdAaW5zdGl0dXRpb24uZWR1"
    out = f"dn: uid=aturing\nmail:: {encoded}\n"
    assert _run(mocker, out) == "alan.turing@institution.edu"


def test_mail_base64_with_padding_whitespace_is_trimmed(mocker):
    """The whitespace is *why* the value was encoded, so it must be stripped."""
    # " alan.turing@institution.edu "
    encoded = "IGFsYW4udHVyaW5nQGluc3RpdHV0aW9uLmVkdSA="
    out = f"dn: uid=aturing\nmail:: {encoded}\n"
    assert _run(mocker, out) == "alan.turing@institution.edu"


def test_plain_mail_still_works(mocker):
    out = "dn: uid=aturing\nmail: alan.turing@institution.edu\n"
    assert _run(mocker, out) == "alan.turing@institution.edu"


def test_missing_mail_attribute_returns_empty(mocker):
    """An entry with no address must stay distinguishable from a decode."""
    assert _run(mocker, "dn: uid=aturing\ncn: Alan Turing\n") == ""


def test_undecodable_base64_returns_empty(mocker):
    assert _run(mocker, "dn: uid=aturing\nmail:: !!!not-base64!!!\n") == ""
