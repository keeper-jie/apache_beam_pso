package mapreduce_pso;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mapreduce_pso extends FileOutputFormat {

    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return null;
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //split all the text to line(particle value)
            String line[] = value.toString().split("\\s");
            for (int i = 0; i < line.length; i++) {
                particle p = particle.string_to_particle(line[i]);
                particle.update_velocity(p);
                particle.update_position(p);
                //without limit the position and velocity
                particle.limit_velocity_position(p);

                p.fitness = particle.fitness_function(p.position);
                if (particle.compare_fitness(p.p_fitness, p.fitness)) {
                    p.p_fitness = p.fitness;
                    p.p_best = p.position.clone();
                }
                //two method:1.map only update p_best,the g_best use reduce to update
//                //        if f(pi) < f(g) then
//                //        Update the swarm's best known position: g ← pi
//                if (particle.compare_fitness(particle.swarm_best_fitness, p.p_fitness)) {
//                    //use global topology,emit the update best position message to others
//                    for (int j = 0; j < p.neighborhood.length; j++) {
//                        IntWritable neighborhood = new IntWritable(p.neighborhood[j]);
//                        Text message = new Text(p.toString());
//                        context.write(neighborhood, message);
//                    }
//                } else {
//                    //no updated position find,emit original particle
//                    IntWritable neighborhood = new IntWritable(p.id);
//                    Text message = new Text(p.toString());
//                    context.write(neighborhood, message);
//                }

                //update particle's best position
                if (particle.compare_fitness(p.particle_best_fitness, p.p_fitness)) {
                    p.particle_best_fitness = p.p_fitness;
                    p.particle_best = p.p_best.clone();
                }

                //send the message to neighborhood
                for (int j = 0; j < p.neighborhood.length; j++) {
                    IntWritable neighborhood = new IntWritable(p.neighborhood[j]);
                    Text message = new Text(p.toString());
                    context.write(neighborhood, message);
                }

                //send itself
                IntWritable neighborhood = new IntWritable(p.id);
                Text message = new Text(p.toString());
                context.write(neighborhood, message);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, Text, NullWritable, Text> {
        particle p = null;
        double particle_best_fitness = Double.MAX_VALUE;
        double[] particle_best;


        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int id = Integer.parseInt(key.toString());
            double swarm_best_fitness = Double.MAX_VALUE;
            double[] swarm_best = new double[]{Double.MAX_VALUE, Double.MAX_VALUE};

            for (Text s : values) {
                particle temp = particle.string_to_particle(String.valueOf(s));
                //set the init particle_best_fitness's value
                particle_best_fitness = temp.p_fitness;
                particle_best = temp.p_best;
                //find the swarm's best fitness in reduce
//                if (particle.compare_fitness(particle.swarm_best_fitness, temp.p_fitness)) {
//                    //update swarm_best_fitness and swam_best
//                    particle.swarm_best_fitness = temp.p_fitness;
//                    particle.swarm_best = temp.p_best;
//                }

                //find the particle's
                if (particle.compare_fitness(swarm_best_fitness, temp.p_fitness)) {
                    //update swarm_best_fitness and swam_best
                    swarm_best_fitness = temp.p_fitness;
                    swarm_best = temp.p_best;
                }
                if (id == temp.id) {
                    //if the p is null,set p as the p and change id to the particle
                    p = temp;
                }
            }

            //change the particle's swarm best to the best value
            p.particle_best_fitness = swarm_best_fitness;
            p.particle_best = swarm_best;
            //write the reduce result
            Text message = new Text(p.toString());
            context.write(NullWritable.get(), message);
        }
    }

    public static void main(String[] args) throws Exception {
        for (int experiment_time = 0; experiment_time < 10; experiment_time++) {
            double startTime = System.nanoTime();

            Configuration conf = new Configuration();

            // configuration should contain reference to your namenode
            FileSystem fs = FileSystem.get(new Configuration());

            int count = 500;
            String input = "/share/word-count-beam/src/main/java/apache_beam_pso/pso_init_2000.txt";
            String output;
            for (int i = 0; i < count; i++) {
                Job job = Job.getInstance(conf, "mapreduce_pso");
                job.setJarByClass(mapreduce_pso.class);
                job.setMapperClass(TokenizerMapper.class);
                job.setReducerClass(IntSumReducer.class);

                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                output = "/share/wordcount/src/main/java/mapreduce_pso/mapreduece_pso_output" + i;
//                output = "/share/wordcount/src/main/java/mapreduce_pso/mapreduece_pso_output";
                // true stands for recursively deleting the folder you gave
                fs.delete(new Path(output), true);

                FileInputFormat.addInputPath(job, new Path(input));
                FileOutputFormat.setOutputPath(job, new Path(output));

//        String ouput_file_name = "mapreduece_pso_output.txt";
//        FileOutputFormat.setOutputName(job, ouput_file_name);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.waitForCompletion(true);
                input = output;
            }
            double endTime = System.nanoTime();
            double duration = (endTime - startTime) / 1000000000;  //divide by 1000000 to get milliseconds.
            String path = "/share/wordcount/src/main/java/mapreduce_pso_time.txt";
            particle.write_file(path, duration);
            System.out.println(duration);
        }
    }
}
