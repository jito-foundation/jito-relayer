use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub fn is_global(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(ip) => is_global_v4(ip),
        IpAddr::V6(ip) => is_global_v6(ip),
    }
}

pub fn is_global_v4(addr: &Ipv4Addr) -> bool {
    // check if this address is 192.0.0.9 or 192.0.0.10. These addresses are the only two
    // globally routable addresses in the 192.0.0.0/24 range.
    let u32_form = u32::from(*addr);
    if u32_form == 0xc0000009 || u32_form == 0xc000000a {
        return true;
    }

    let octs = addr.octets();

    !addr.is_private()
        && !addr.is_loopback()
        && !addr.is_link_local()
        && !addr.is_broadcast()
        && !addr.is_documentation()
        && !is_shared_v4(&octs)
        && !is_ietf_protocol_assignment_v4(&octs)
        && !is_reserved_v4(*addr, &octs)
        && !is_benchmarking_v4(&octs)
        && octs[0] != 0
}

fn is_benchmarking_v4(octs: &[u8]) -> bool {
    octs[0] == 198 && (octs[1] & 0xfe) == 18
}

fn is_reserved_v4(addr: Ipv4Addr, octs: &[u8]) -> bool {
    octs[0] & 240 == 240 && !addr.is_broadcast()
}

fn is_ietf_protocol_assignment_v4(octs: &[u8]) -> bool {
    octs[0] == 192 && octs[1] == 0 && octs[2] == 0
}

fn is_shared_v4(octs: &[u8]) -> bool {
    octs[0] == 100 && (octs[1] & 0b1100_0000 == 0b0100_0000)
}

pub fn is_global_v6(addr: &Ipv6Addr) -> bool {
    if addr.is_multicast() {
        match addr.segments()[0] & 0x000f {
            14 => true, // Ipv6MulticastScope::Global
            _ => false,
        }
    } else {
        !addr.is_loopback()
            && !is_unicast_link_local_v6(*addr)
            && !is_unique_local_v6(*addr)
            && !addr.is_unspecified()
            && !is_documentation_v6(*addr)
    }
}

fn is_unicast_link_local_v6(addr: Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xffc0) == 0xfe80
}

fn is_unique_local_v6(addr: Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xfe00) == 0xfc00
}

fn is_documentation_v6(addr: Ipv6Addr) -> bool {
    (addr.segments()[0] == 0x2001) && (addr.segments()[1] == 0xdb8)
}
