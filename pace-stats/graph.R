times <- read.table("times.csv",sep=",", header=TRUE)

times100k <- times[times$size == 100000,]
times1000k <- times[times$size == 1000000,]
times10000k <- times[times$size == 10000000,]


timesSize <- times[times$window==100,]


p100k <- ggplot(times100k, aes(window), ylabel="") + 
  geom_line(aes(y = precision, linetype="precision"))  +
  geom_line(aes(y = recall, linetype = "recall")) + scale_y_continuous("",limits = c(0, 1)) +
  labs(linetype = "") 

ggsave(filename="/tmp/p100k.pdf", height=4, width=7)


p1000k <-  ggplot(times1000k, aes(window)) + 
  geom_line(aes(y = precision, linetype = "precision")) + 
  geom_line(aes(y = recall, linetype = "recall")) + scale_y_continuous("", limits = c(0, 1)) +
  labs(linetype = "") 

ggsave(filename="/tmp/p1000k.pdf", height=4, width=7)


p10000k <-  ggplot(times10000k, aes(window)) + 
  geom_line(aes(y = precision, linetype = "precision")) + 
  geom_line(aes(y = recall, linetype = "recall")) + scale_y_continuous("", limits = c(0, 1)) +
  labs(linetype = "") 

ggsave(filename="/tmp/p10000k.pdf", height=4, width=7)


t100k <- ggplot(times100k, aes(window)) + geom_line(aes(y = time))
ggsave(filename="/tmp/t100k.pdf", height=4, width=7)

t1000k <- ggplot(times1000k, aes(window)) + geom_line(aes(y = time))
ggsave(filename="/tmp/t1000k.pdf", height=4, width=7)

#sizes <- read.table("sizes.csv",sep=",",header=TRUE)
s <- ggplot(timesSize, aes(size)) + geom_line(aes(y = time)) + scale_x_log10() + scale_y_log10()

ggsave(filename="/tmp/sizes.pdf", height=4, width=7)

cores <- read.table("cores.csv",sep=",",header=TRUE)
c <- ggplot(cores, aes(cores)) + geom_line(aes(y = time))

ggsave(filename="/tmp/cores.pdf", height=4, width=7)

cores$speedup = (1428/4)/cores$time
c <- ggplot(cores, aes(cores)) + geom_line(aes(y = speedup))
speedup <- c

ggsave(filename="/tmp/speedup.pdf", height=4, width=7)
