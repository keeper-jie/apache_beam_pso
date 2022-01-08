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

/*
* mapreduce pso class(including map and reduce class)
*/
public class mapreduce_pso {
    /*
    * mapreduce_pso map sphere
    * input: <Object, Text>
    * output: <IntWritable, Text>
    */
    public static class PSOMapper
            extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //split all the text to line(particle value)
            String line[] = value.toString().split("\\s");
            for (int i = 0; i < line.length; i++) {
                particle p = particle.string_to_particle(line[i]);
                particle.update_velocity(p);
                particle.update_position(p);
                particle.limit_velocity_position(p);

                p.fitness = particle.fitness_function(p.position);
                if (particle.compare_fitness(p.p_fitness, p.fitness)) {
                    p.p_fitness = p.fitness;
                    p.p_best = p.position.clone();
                }
                //update particle's best position
                if (particle.compare_fitness(p.particle_best_fitness, p.p_fitness)) {
                    p.particle_best_fitness = p.p_fitness;
                    p.particle_best = p.p_best.clone();
                    //send the message to neighborhood
                    for (int j = 0; j < p.neighborhood.length; j++) {
                        IntWritable neighborhood = new IntWritable(p.neighborhood[j]);
                        Text message = new Text(p.toString());
                        context.write(neighborhood, message);
                    }
                }
                //send particle itself(the neighborhood id list do not have the particle's id)e
                IntWritable neighborhood = new IntWritable(p.id);
                Text message = new Text(p.toString());
                context.write(neighborhood, message);
            }
        }
    }

    /*
     * mapreduce_pso reduce sphere
     * input: <IntWritable, Text>
     * output: <NullWritable, Text>
     */
    public static class PSOReducer
            extends Reducer<IntWritable, Text, NullWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int id = Integer.parseInt(key.toString());
            particle p = null;
            double swarm_best_fitness = Double.MAX_VALUE;
            double[] swarm_best = new double[]{};

            for (Text s : values) {
                particle temp = particle.string_to_particle(String.valueOf(s));
                if (particle.compare_fitness(swarm_best_fitness, temp.p_fitness)) {
                    //update swarm_best_fitness and swam_best
                    swarm_best_fitness = temp.p_fitness;
                    swarm_best = temp.p_best;
                }
                if (id == temp.id) {
                    p = temp;
                }
            }

            //change the particle's swarm best to the best value of list
            p.particle_best_fitness = swarm_best_fitness;
            p.particle_best = swarm_best;
            //write the reduce result
            Text message = new Text(p.toString());
            context.write(NullWritable.get(), message);
        }
    }

    /*
    * main
    */
    public static void main(String[] args) throws Exception {
        for (int experiment_time = 0; experiment_time < 5; experiment_time++) {
            double startTime = System.nanoTime();
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new Configuration());

            //iteration times
            int count = 1000;
            String input = "/share/word-count-beam/src/main/java/apache_beam_pso/pso_init_generalized_griewank_500particle_30dimension.txt";
            String output = null;
            for (int i = 0; i < count; i++) {
                Job job = Job.getInstance(conf, "mapreduce_pso");
                job.setJarByClass(mapreduce_pso.class);
                job.setMapperClass(PSOMapper.class);
                job.setReducerClass(PSOReducer.class);

                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                output = "/share/wordcount/src/main/java/output_pso_init_generalized_griewank_500particle_30dimension"+experiment_time+"/mapreduece_pso_output_" + i;
                fs.delete(new Path(output), true);
                FileInputFormat.addInputPath(job, new Path(input));
                FileOutputFormat.setOutputPath(job, new Path(output));

                job.waitForCompletion(true);
                input = output;
            }
            double endTime = System.nanoTime();
            double duration = (endTime - startTime) / 1000000000;  //divide by 1000000000 to get seconds.
            String path = "/share/wordcount/src/main/java/time_pso_init_generalized_griewank_500particle_30dimension.txt";
            particle.write_file(path, duration, true);
            System.out.println(duration);
        }
    }
}
