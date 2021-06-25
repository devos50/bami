library(ggplot2)

data_summary <- function(data, varname, groupnames){
  require(plyr)
  summary_func <- function(x, col){
    c(mean = mean(x[[col]], na.rm=TRUE),
      sd = sd(x[[col]], na.rm=TRUE))
  }
  data_sum<-ddply(data, groupnames, .fun=summary_func,
                  varname)
  data_sum <- rename(data_sum, c("mean" = varname))
 return(data_sum)
}

dat <- read.csv("../data/frontier_stats.csv")
dat$peer <- as.factor(dat$peer)

dat2 <- data_summary(dat, varname="terminal_size", groupnames=c("time"))

plot <- ggplot(data=dat2, aes(x=time, y=terminal_size)) +
        geom_line() +
        geom_errorbar(aes(ymin=terminal_size-sd, ymax=terminal_size+sd), width=.2, position=position_dodge(0.05)) +
        theme_bw() +
        xlab("Time into experiment (s.)") +
        ylab("Number of terminal nodes") +
        theme(legend.position="none")

ggsave(filename="terminal_sizes.pdf", plot=plot, height=4, width=8)
