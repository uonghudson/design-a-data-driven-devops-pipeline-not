# Load necessary packages
library(RPostgres)
library(ggplot2)
library(plumber)

# Establish database connection
con <- dbConnect(
  drv = "postgres",
  dbname = "mydatabase",
  host = "localhost",
  port = 5432,
  user = "myuser",
  password = "mypassword"
)

# Define a function to query database and generate notification data
get_notification_data <- function() {
  data <- dbGetQuery(con, "SELECT * FROM pipeline_notifications WHERE notified = FALSE")
  return(data)
}

# Define a function to send notification via email
send_notification <- function(data) {
  # TO DO: implement email sending logic using a package like mailR
  cat("Sending notification...")
  print(data)
}

# Create a plumber API to expose notification endpoint
pipeline_notifier_api <- plumb("pipeline_notifier_api")
pipeline_notifier_api$get("/notifications", function(req, res) {
  data <- get_notification_data()
  if (nrow(data) > 0) {
    send_notification(data)
    dbExecute(con, "UPDATE pipeline_notifications SET notified = TRUE WHERE notified = FALSE")
    res$status <- 200
    res$body <- "Notifications sent successfully!"
  } else {
    res$status <- 204
    res$body <- "No new notifications."
  }
})

# Run the plumber API
pipeline_notifier_api$run(host = "localhost", port = 8080)