package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import apache_beam_pso.particle;

import java.io.IOException;


public class apache_beam_pso {

    public static class map_pso extends DoFn<String, KV<Integer, String>> {

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<Integer, String>> receiver) {
            //convert line to particle
            particle p = particle.string_to_particle(element);
            //update_velocity
            particle.update_velocity(p);
            //update_position
            particle.update_position(p);
            //limit_velocity_position
            particle.limit_velocity_position(p);

            p.fitness = particle.fitness_function(p.position);
            //        if f(xi) < f(pi) then
            //        Update the particle's best known position: pi ← xi
            if (particle.compare_fitness(p.p_fitness, p.fitness)) {
                p.p_fitness = p.fitness;
                p.p_best = p.position.clone();
            }
            //two method:1.map only update p_best,the g_best use reduce to update
//                //        if f(pi) < f(g) then
//                //        Update the swarm's best known position: g ← pi
            //compare with static swarm_best_fitness
//            if (particle.compare_fitness(particle.swarm_best_fitness, p.p_fitness)) {
//                //use global topology,emit the update best position message to others
//                for (int i = 0; i < p.neighborhood.length; i++) {
//                    receiver.output(KV.of(p.neighborhood[i], p.toString()));
//                }
//            } else {
//                //no updated position find,emit original particle
//                receiver.output(KV.of(p.id, p.toString()));
//            }

            //compare with particle's particle_best_fitness
            if (particle.compare_fitness(p.particle_best_fitness, p.p_fitness)) {
                //update particle_best_fitness
                p.particle_best_fitness = p.p_fitness;
                p.particle_best = p.p_best.clone();
            }
            //emit message to neighborhood
            for (int i = 0; i < p.neighborhood.length; i++) {
                receiver.output(KV.of(p.neighborhood[i], p.toString()));
            }
            //emit particle itself
            receiver.output(KV.of(p.id, p.toString()));
        }
    }

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class reduce_pso extends DoFn<KV<Integer, Iterable<String>>, String> {
        //use p to find the reduce particle
        particle p = null;
        double swarm_best_fitness = Double.MAX_VALUE;
        double[] swarm_best_position;

        @ProcessElement
        public void processElement(ProcessContext c) {
            //current key as particle's id
            Integer id = c.element().getKey();
            Iterable<String> group_by_id_particles = c.element().getValue();
            //update the swarm_g_best by key,find the g_best
            for (String s : group_by_id_particles) {
                particle temp = particle.string_to_particle(s);
//                //set the init particle_best_fitness's value
//                particle_best_fitness = temp.p_fitness;
//                particle_best = temp.p_best;
                //find the static swarm best fitness in reduce
//                if (particle.compare_fitness(particle.swarm_best_fitness, temp.p_fitness)) {
//                    //update swarm_best_fitness and swam_best
//                    particle.swarm_best_fitness = temp.p_fitness;
//                    particle.swarm_best = temp.p_best;
//                }

                //store the particle's particle_best_fitness
                if (particle.compare_fitness(swarm_best_fitness, temp.particle_best_fitness)) {
                    swarm_best_fitness = temp.particle_best_fitness;
                    swarm_best_position = temp.particle_best;
                }
                if (id == temp.id) {
                    //if the p is null,set p as the p and change id to the particle
                    p = temp;
                }
            }
            if (particle.compare_fitness(p.particle_best_fitness, swarm_best_fitness)) {
                p.particle_best_fitness = swarm_best_fitness;
                p.particle_best = swarm_best_position;
            }
            //updated swarm_best_fitness and output
            c.output(p.toString());
        }
    }

    public static class CountWords
            extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> lines) {
            //map:convert lines of text into particle's and update each particle
            PCollection<KV<Integer, String>> map_pso = lines.apply(ParDo.of(new map_pso()));
            //shuffle
            PCollection<KV<Integer, Iterable<String>>> shuffle_pso = map_pso.apply(GroupByKey.<Integer, String>create());
            //reduce
            PCollection<String> reduce_pso =
                    shuffle_pso.apply(ParDo.of(new reduce_pso()));
            return reduce_pso;
        }
    }

    public interface WordCountOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("/share/word-count-beam/src/main/java/apache_beam_pso/pso_init.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("/share/word-count-beam/src/main/java/org/apache/beam/examples/beam_pso_output.txt")
        String getOutput();

        void setOutput(String value);
    }

    static void runWordCount(WordCountOptions options) throws IOException {
//        for (int experiment_time = 0; experiment_time < 10; experiment_time++) {
            double startTime = System.nanoTime();
            Pipeline p = Pipeline.create(options);
            String input_pso = "/share/word-count-beam/src/main/java/apache_beam_pso/pso_init.txt";
            //init pso
            PCollection<String> init_pso = p.apply("ReadLines", TextIO.read().from(input_pso));

            double iteration = 500;
            for (int i = 0; i < iteration; i++) {
                //iteration
                init_pso = init_pso.apply(new CountWords());
//            init_pso.apply("WriteCounts", TextIO.write().withoutSharding().to(output_pso));
//            init_pso.apply("result_output", TextIO.write().withoutSharding().to(output_pso));
            }
//        String output_pso = "/share/word-count-beam/src/main/java/org/apache/beam/examples/beam_pso_output" + i + ".txt";
            String output_pso = "/share/word-count-beam/src/main/java/org/apache/beam/examples/beam_pso_output.txt";
            init_pso.apply("result_output", TextIO.write().withoutSharding().to(output_pso));
            p.run().waitUntilFinish();

            double endTime = System.nanoTime();
            double duration = (endTime - startTime) / 1000000000;  //divide by 1000000000 to get seconds.
            String path = "/share/word-count-beam/src/main/java/org/apache/beam/examples/apache_beam_pso_time.txt";
            particle.write_file(path, duration);
            System.out.println(duration);
//        }
    }

    public static void main(String[] args) throws IOException {
        WordCountOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);
        runWordCount(options);
    }
}
