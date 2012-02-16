times <- read.table("times.csv",sep=",", header=TRUE)

times100k <- times[times$size == 100000,]
times1000k <- times[times$size == 1000000,]

p100k <- ggplot(times100k, aes(window), ylabel="") + 
  geom_line(aes(y = precision, linetype="precision"))  +
  geom_line(aes(y = recall, linetype = "recall")) + scale_y_continuous("",limits = c(0, 1)) +
  labs(linetype = "") 

ggsave(filename="/tmp/p100k.eps", height=4, width=7)


p1000k <-  ggplot(times1000k, aes(window)) + 
  geom_line(aes(y = precision, linetype = "precision")) + 
  geom_line(aes(y = recall, linetype = "recall")) + scale_y_continuous("", limits = c(0, 1)) +
  labs(linetype = "") 

ggsave(filename="/tmp/p1000k.eps", height=4, width=7)


t100k <- ggplot(times100k, aes(window)) + geom_line(aes(y = time))
ggsave(filename="/tmp/t100k.eps", height=4, width=7)

t1000k <- ggplot(times1000k, aes(window)) + geom_line(aes(y = time))
ggsave(filename="/tmp/t1000k.eps", height=4, width=7)

sizes <- read.table("sizes.csv",sep=",",header=TRUE)
s <- ggplot(sizes, aes(size)) + geom_line(aes(y = time)) + scale_x_log10() + scale_y_log10()

ggsave(filename="/tmp/sizes.eps", height=4, width=7)

cores <- read.table("cores.csv",sep=",",header=TRUE)
c <- ggplot(cores, aes(cores)) + geom_line(aes(y = time))

ggsave(filename="/tmp/cores.eps", height=4, width=7)


hashes1000k <- read.table("hashes-1000k.csv",sep=",", header=TRUE)

ph1000k <-  ggplot(hashes1000k, aes(window)) + 
  geom_line(aes(y = precision, linetype = "precision")) + 
  geom_line(aes(y = recall, linetype = "recall")) + scale_y_continuous("", limits = c(0, 1)) +
  labs(linetype = "") 

ggsave(filename="/tmp/ph1000k.eps", height=4, width=7)

th1000k <- ggplot(hashes1000k, aes(window)) + geom_line(aes(y = time))

ggsave(filename="/tmp/th1000k.eps", height=4, width=7)


shashes1000k <- read.table("shashes-1000k.csv",sep=",", header=TRUE)

psh1000k <-  ggplot(shashes1000k, aes(window)) + 
  geom_line(aes(y = precision, linetype = "precision")) + 
  geom_line(aes(y = recall, linetype = "recall")) + scale_y_continuous("", limits = c(0, 1)) +
  labs(linetype = "") 

ggsave(filename="/tmp/psh1000k.eps", height=4, width=7)

tsh1000k <- ggplot(shashes1000k, aes(window)) + geom_line(aes(y = time))

ggsave(filename="/tmp/tsh1000k.eps", height=4, width=7)

