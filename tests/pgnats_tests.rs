use testcontainers::{
  core::{ContainerPort, WaitFor},
  runners::SyncRunner,
  Container, GenericImage, ImageExt,
};

#[must_use]
fn _setup() -> (Container<GenericImage>, u16) {
  let container = testcontainers::GenericImage::new("nats", "latest")
    .with_exposed_port(ContainerPort::Tcp(4222))
    .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
    .with_cmd(["-js"])
    .start()
    .expect("Failed to start NATS server");

  let host_port = container
    .get_host_port_ipv4(ContainerPort::Tcp(4222))
    .expect("Failed to get host port");

  (container, host_port)
}
