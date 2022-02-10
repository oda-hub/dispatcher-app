from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"

import  numpy  as np

from astropy import wcs

from bokeh.models import FuncTickFormatter, FixedTicker

from bokeh.layouts import row, column, gridplot
from bokeh.models import (CustomJS,
                          Toggle, 
                          RangeSlider,
                          HoverTool,
                          ColorBar,
                          LinearColorMapper,
                          LabelSet,
                          ColumnDataSource,
                          LogColorMapper)
from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.palettes import Plasma256


class Image(object):

    def __init__(self,data,header):
        self.data=data
        self.header=header

    def get_html_draw(self,
                      w=None,
                      h=None, 
                      catalog=None, 
                      x_scale = "linear", 
                      y_scale = "linear", 
                      x_range = None,
                      y_range = None,
                      x0 = 0,
                      y0 = 0,
                      dw = None,
                      dh = None,
                      x_label = None,
                      y_label = None,
                      enable_log_cmap = True,
                      ):

        msk = ~np.isnan(self.data)

        min_v = self.data[msk].min()
        max_v = self.data[msk].max()

        if x_range is None:
            c = self.data.shape[1]
            x_range = (0, c)
        if y_range is None:
            r = self.data.shape[0]
            y_range=(0, r)
        if dw is None:
            dw = c
        if dh is None:
            dh = r

        fig = figure(plot_width = w, 
                     plot_height = h, 
                     x_axis_type = x_scale,
                     y_axis_type = y_scale,
                     x_range=x_range, 
                     y_range=y_range,
                     x_axis_label = x_label,
                     y_axis_label = y_label,
                     tools=['pan,box_zoom,box_select,wheel_zoom,reset,save,crosshair'])

        lin_color_mapper = LinearColorMapper(low = min_v, high = max_v, palette='Plasma256')
        
        fig_im = fig.image(image=[self.data], x=x0, y=y0, dw=dw, dh=dh,
                           color_mapper=lin_color_mapper)

        hover = HoverTool(tooltips=[("x", "$x"), ("y", "$y"), ("value", "@image")],
                          renderers=[fig_im])

        fig.add_tools(hover)

        if catalog is not None:

            lon = catalog.ra
            lat = catalog.dec

            cur_wcs = wcs.WCS(self.header)

            if len(lat) > 0.:
                pixcrd = cur_wcs.wcs_world2pix(np.column_stack((lon, lat)), 0)

                msk = ~np.isnan(pixcrd[:, 0])
                source = ColumnDataSource(data=dict(lon=pixcrd[:, 0][msk]+0.5,
                                                    lat=pixcrd[:, 1][msk]+0.5,
                                                    names=catalog.name[msk]))

                fig.scatter(x='lon', y='lat', marker='circle', size=15,
                            line_color="white", fill_color=None, alpha=1.0, source=source)

                labels = LabelSet(x='lon', y='lat', text='names', level='glyph',
                                  x_offset=5, y_offset=5, render_mode='canvas', source=source, text_color='white')

                fig.add_layout(labels)


        lin_color_bar = ColorBar(color_mapper = lin_color_mapper, label_standoff=12, border_line_color=None, location=(0, 0), width=15)

        fig.add_layout(lin_color_bar, 'right')
        
        graph_slider = RangeSlider(title='Sig. Range', start = min_v, end = max_v, step=(max_v - min_v) / 1000, value = (min_v, max_v * 0.8))
        
        graph_slider.js_link('value', lin_color_mapper, 'low', attr_selector = 0)
        graph_slider.js_link('value', lin_color_mapper, 'high', attr_selector = 1)

        widgets = [graph_slider]
        
        if enable_log_cmap:
            log_color_mapper = LogColorMapper(low = max(0, min_v), high = max_v, palette=Plasma256)
            graph_slider.js_link('value', log_color_mapper, 'low', attr_selector = 0)
            graph_slider.js_link('value', log_color_mapper, 'high', attr_selector = 1)
            
            log_color_bar = ColorBar(color_mapper = log_color_mapper, label_standoff=2, border_line_color=None, location=(0, 0), width=15)
            fig.add_layout(log_color_bar, 'right')
            log_color_bar.visible = False


            log_toggle = Toggle(label='Toggle Log. Norm', active=False)
            log_toggle.js_on_click(CustomJS(args = dict(lin_color_mapper = lin_color_mapper, 
                                                        log_color_mapper = log_color_mapper, 
                                                        fig_im = fig_im,
                                                        graph_slider = graph_slider,
                                                        min_v = min_v,
                                                        lin_color_bar = lin_color_bar,
                                                        log_color_bar = log_color_bar),
                                            code = """
                                                if (this.active) {
                                                    graph_slider.value = [Math.max(0 + graph_slider.step, graph_slider.value[0]), graph_slider.value[1]];
                                                    graph_slider.start = Math.max(0 + graph_slider.step, min_v);
                                                    fig_im.glyph.color_mapper = log_color_mapper;
                                                    log_color_bar.visible = true;
                                                    lin_color_bar.visible = false;
                                                } else {
                                                    graph_slider.start = min_v;
                                                    fig_im.glyph.color_mapper = lin_color_mapper;
                                                    log_color_bar.visible = false;
                                                    lin_color_bar.visible = true;
                                                }
                                                """
                                            )
                                )
            widgets.append(log_toggle)
            
        layout = column(row(widgets), 
                        fig)

        script, div = components(layout)

        html_dict = {}
        html_dict['script'] = script
        html_dict['div'] = div
        return html_dict

    def get_js9_html(self, file_path,region_file=None,js9_id='myJS9'):
        region=''
        file='''JS9.Preload("product/%s", {scale: 'linear', colormap: 'plasma'}, {display: "%s"});'''%(file_path,js9_id)
        if region_file is not None:
            region='''JS9.LoadRegions("product/%s", {display: "%s"});\n'''%(region_file,js9_id)
        t = '''                                                                                                                                                                             
    <html>                                                                                                                                                                                     
                <head>                                                                                                                                                                         
                  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">                                                                                                          
                  <meta http-equiv="X-UA-Compatible" content="IE=Edge;chrome=1" >                                                                                                              
                  <meta name="viewport" content="width=device-width, initial-scale=1">                                                                                                         
                  <link type="image/x-icon" rel="shortcut icon" href="./favicon.ico">                                                                                                          
                  <link type="text/css" rel="stylesheet" href="js9/js9support.css">                                                                                                            
                  <link type="text/css" rel="stylesheet" href="js9/js9.css">                                                                                                                   
                  <script type="text/javascript" src="js9/js9prefs.js"></script>                                                                                                               
                  <script type="text/javascript" src="js9/js9support.min.js"></script>                                                                                                         
                  <script type="text/javascript" src="js9/js9.min.js"></script>                                                                                                                
                  <script type="text/javascript" src="js9/js9plugins.js"></script>                                                                                                             
                    </head>                                                                                                                                                                    
                <body>                                                                                                                                                                         


                <center><font size="+1">                                                                                                                                                       
                </font></center>                                                                                                                                                               
                <table cellspacing="30">                                                                                                                                                       
                <tr valign="top">                                                                                                                                                              
        <td>                                                                                                                                                                                   
                </td>                                                                                                                                                                          
        <td>                                                                                                                                                                                   
                <tr valign="top">                                                                                                                                                              
                <td>                                                                                                                                                                           
                <div class="JS9Menubar"  id="%sMenubar" ></div>
                <div class="JS9Colorbar" id="%sColorbar" ></div>
                <div class="JS9" id="%s"></div>                                                                                                                                                        
                </td>                                                                                                                                                                          
                <td>                                                                                                                                                                           

                <p>                                                                                                                                                                            
                </td>                                                                                                                                                                          
                </tr>                                                                                                                                                                          
                </table>                                                                                                                                                                       
                <script type="text/javascript">                                                                                                                                                
                  function init(){                                                                                                                                                             
                     var idx, obj;                                                                                                                                                             
                     %s
                     %s                                                                                                                      
                  }                                                                                                                                                                            
                  $(document).ready(function(){                                                                                                                                                
                    init();                                                                                                                                                                    
                  });                                                                                                                                                                          
                </script>                                                                                                                                                                      

            </body>                                                                                                                                                                            
    </html>                                                                                                                                                                                    

    ''' % (js9_id,js9_id,js9_id,file,region)

        return t

class ScatterPlot(object):


    def __init__(self,w,h,x_label=None,y_label=None,x_range=None,y_range=None,title=None,y_axis_type='linear',x_axis_type='linear'):
        hover = HoverTool(tooltips=[("x", "$x"), ("y", "$y")])

        self.fig = figure(title=title, width=w, height=h,x_range=x_range,y_range=y_range,
                          y_axis_type=y_axis_type,
                          x_axis_type=x_axis_type,
                     tools=[hover, 'pan,box_zoom,box_select,wheel_zoom,reset,save,crosshair']
                     )

        if x_label is not None:
            self.fig.xaxis.axis_label = x_label

        if y_label is not None:
            self.fig.yaxis.axis_label = y_label

    def add_errorbar(self, x, y, xerr=None, yerr=None, color='red',
                 point_kwargs={}, error_kwargs={}):

        self.fig.circle(x, y, color=color, **point_kwargs)

        if xerr is not None:
            x_err_x = []
            x_err_y = []
            for px, py, err in zip(x, y, xerr):
                x_err_x.append((px - err, px + err))
                x_err_y.append((py, py))
            self.fig.multi_line(x_err_x, x_err_y, color=color, **error_kwargs)

        if yerr is not None:
            y_err_x = []
            y_err_y = []
            for px, py, err in zip(x, y, yerr):
                y_err_x.append((px, px))
                y_err_y.append((py - err, py + err))
            self.fig.multi_line(y_err_x, y_err_y, color=color, **error_kwargs)



    def add_step_line(self,x,y,legend=None):
        #print('a')
        self.fig.step(x,y,name=legend, mode="center")
        #print('b')

    def add_line(self,x,y,legend=None,color='red'):
        self.fig.line(x,y,name=legend,line_color=color)

    def get_html_draw(self):



        layout = row(
            self.fig
        )
        #curdoc().add_root(layout)


        #show(layout)

        script, div = components(layout)

        #print ('script',script)
        #print ('div',div)

        html_dict = {}
        html_dict['script'] = script
        html_dict['div'] = div
        return html_dict


class GridPlot(object):

    def __init__(self,f1,f2,w=None,h=None):

        self.f1=f1
        self.f2=f2

    def get_html_draw(self,w=None,h=None):
        #l = layout([self.f1.fig],[self.f2.fig])


        grid = gridplot([self.f1.fig,self.f2.fig],ncols=1,plot_width=w, plot_height=h)
        #curdoc().add_root(grid)
        #show(grid)
        #output_file("test.html")
        script, div = components(grid)

        html_dict={}
        html_dict['script']=script
        html_dict['div'] = div
        return html_dict
