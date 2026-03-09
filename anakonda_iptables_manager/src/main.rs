use futures::TryStreamExt;
use ipnet::IpNet;
use iptables::IPTables;
use k8s_openapi::Metadata;
use k8s_openapi::api::core::v1::{Pod, PodCondition};
use kube::api::ListParams;
use kube::{Api, Resource, ResourceExt};
use kube_runtime::{WatchStreamExt, watcher};
use log::{debug, error, info};
use std::env;
use std::net::IpAddr;
use tokio::runtime::Runtime;

fn add_initial_rules(ipt: &IPTables, node_cidr: &String) {
    let rules = vec![
        // FORWARD chain: reject all TCP traffic to NODE_CIDR:8888 coming from other nodes
        ("filter", "FORWARD", "-p tcp -d {ip} --dport 8888 -j REJECT"),
        // OUTPUT chain: reject all TCP traffic to NODE_CIDR:8888 coming from within the node
        ("filter", "OUTPUT", "-p tcp -d {ip} --dport 8888 -j REJECT"),
        // OUTPUT chain: allow all local TCP traffic to NODE_CIDR:8888 for UID anakonda
        (
            "filter",
            "OUTPUT",
            "-p tcp -d {ip} --dport 8888 -m owner --uid-owner anakonda -j ACCEPT",
        ),
        // OUTPUT chain: allow all local TCP traffic to NODE_CIDR:8888 for UID root
        (
            "filter",
            "OUTPUT",
            "-p tcp -d {ip} --dport 8888 -m owner --uid-owner root -j ACCEPT",
        ),
    ];

    for (table, chain, rule_template) in rules {
        let rule = rule_template.replace("{ip}", node_cidr);

        match ipt.exists(table, chain, &rule) {
            Ok(false) => match ipt.insert_unique(table, chain, &rule, 1) {
                Ok(_) => info!("Inserted GENERAL rule: {} {} {}", table, chain, rule),
                Err(e) => {
                    eprintln!(
                        "Error inserting GENERAL rule: {} {} {} {}",
                        table, chain, rule, e
                    )
                }
            },
            Ok(true) => {
                info!("GENERAL rule already existed: {} {} {}", table, chain, rule);
            }
            Err(e) => eprintln!(
                "Error checking for rule existence: {} {} {} {}",
                table, chain, rule, e
            ),
        }
    }
}

fn edit_iptables_rules(
    ipt: &IPTables,
    pod_ip: String,
    remove: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Define rules
    let rules = vec![
        // FORWARD chain: accept all TCP traffic originating at other nodes and
        // destined to pods that are not workloads (POD_IP:8888)
        ("filter", "FORWARD", "-p tcp -d {ip} --dport 8888 -j ACCEPT"),
        // OUTPUT chain: accept all TCP traffic originating at the same node and
        // destined to pods that are not workloads (POD_IP:8888)
        ("filter", "OUTPUT", "-p tcp -d {ip} --dport 8888 -j ACCEPT"),
    ];

    for (table, chain, rule_template) in rules {
        let rule = rule_template.replace("{ip}", &pod_ip);

        // Check for duplication
        match ipt.exists(table, chain, &rule) {
            Ok(false) => {
                if !remove {
                    // Insert at position 1
                    match ipt.insert_unique(table, chain, &rule, 1) {
                        Ok(_) => info!("Inserted SPECIFIC rule: {} {} {}", table, chain, rule),
                        Err(e) => {
                            eprintln!(
                                "Error inserting SPECIFIC rule: {} {} {} {}",
                                table, chain, rule, e
                            )
                        }
                    }
                }
            }
            Ok(true) => {
                if remove {
                    match ipt.delete(table, chain, &rule) {
                        Ok(_) => info!("Removed SPECIFIC rule: {} {} {}", table, chain, rule),
                        Err(e) => {
                            eprintln!(
                                "Error deleting SPECIFIC rule: {} {} {} {}",
                                table, chain, rule, e
                            )
                        }
                    }
                } else {
                    info!("SPECIFIC Rule already exists: {} {} {}", table, chain, rule);
                }
            }
            Err(e) => eprintln!(
                "Error checking for SPECIFIC rule existence: {} {} {} {}",
                table, chain, rule, e
            ),
        }
    }

    Ok(())
}

fn ip_in_cidr(ip: &str, cidr: &str) -> bool {
    let ip: IpAddr = ip.parse().unwrap();
    let net: IpNet = cidr.parse().unwrap();
    net.contains(&ip)
}

async fn knative_k8s_informer(
    ipt: &IPTables,
    node_name: &String,
    node_cidr: &String,
) -> anyhow::Result<()> {
    let client = kube::Client::try_default().await?;
    let api = Api::<Pod>::default_namespaced(client);
    let wc = watcher::Config::default().fields(&format!("spec.nodeName={}", node_name));

    info!("Starting K8s pod informer (for namespace 'default')...");

    watcher(api, wc)
        .applied_objects()
        .default_backoff()
        .try_for_each({
            move |pod| async move {
                if let Some(labels) = pod.metadata.labels {
                    let function = labels.get("anakonda.block-extrenal");

                    if function.is_none() || function.unwrap().to_lowercase() != "true" {
                        if let Some(conditions) = pod.status.clone().unwrap().conditions {
                            let ready = conditions
                                .iter()
                                .filter(|c| c.type_ == "Ready")
                                .any(|c| c.status == "True");

                            let phase = pod.status.clone().unwrap().phase.clone().unwrap();
                            let is_terminating = pod.metadata.deletion_timestamp.is_some();
                            let is_running = String::from(phase.clone()) == String::from("Running");

                            debug!(
                                "ready: {}; phase: {}; is_running: {}; is_terminating: {}",
                                ready, phase, is_running, is_terminating
                            );

                            if let Some(status) = pod.status {
                                if let Some(url) = status.pod_ip {
                                    debug!("ip: {}", url);

                                    if ip_in_cidr(&*url, node_cidr) {
                                        if ready && is_running && !is_terminating {
                                            edit_iptables_rules(ipt, url, false)
                                                .expect("Error while adding iptables rules");
                                        } else if !ready && is_running && is_terminating {
                                            edit_iptables_rules(ipt, url, true)
                                                .expect("Error while removing iptables rules");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
        })
        .await?;

    Ok(())
}

fn get_iptables() -> IPTables {
    IPTables {
        cmd: "iptables-legacy",
        has_check: true,
        has_wait: false,
        is_numeric: false,
    }
}

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        error!("Usage: anakonda_iptables_manager NODE_NAME NODE_CIDR");
        return;
    }

    let node_name = &args[1];
    let node_cidr = &args[2];

    info!("Node name is {}", node_name);
    info!("Node CIDR is {}", node_cidr);

    let ipt = get_iptables();
    let runtime = Runtime::new().unwrap();

    add_initial_rules(&ipt, node_cidr);

    runtime.block_on(async {
        if let Err(e) = knative_k8s_informer(&ipt, node_name, node_cidr).await {
            error!("{:?}", e);
        }
    })
}
