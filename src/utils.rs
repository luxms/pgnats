pub fn format_message(message_text: impl AsRef<str>) -> String {
  format!("PGNATS: {}", message_text.as_ref())
}

pub fn do_panic_with_message(message_text: impl AsRef<str>) -> ! {
  panic!("PGNATS: {}", message_text.as_ref())
}
